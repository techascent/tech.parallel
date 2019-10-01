(ns tech.parallel
  (:require [clojure.core.async :as async]
            [tech.parallel.for :as pfor]
            [tech.parallel.require]
            [tech.parallel.next-item-fn]
            [tech.parallel.utils :as utils]
            [clojure.tools.logging :as log])
  (:import [java.util.concurrent ForkJoinPool Callable Future ExecutorService
            ForkJoinPool$ForkJoinWorkerThreadFactory]
           [java.util ArrayDeque PriorityQueue Comparator])
  (:refer-clojure :exclude [memoize]))


(defn- deque-seq
  [^ArrayDeque deque input-seq ^long buffer-depth]
  (let [input-seq
        (loop [deque-size (.size deque)
               input-seq input-seq]
          (if (and (< deque-size buffer-depth)
                     (seq input-seq))
            (let [seq-item (first input-seq)]
              (.add deque seq-item)
              (recur (.size deque)
                     (rest input-seq)))
            input-seq))]
    (when (> (.size deque) 0)
      (let [first-item (.remove deque)]
        (cons first-item (lazy-seq (deque-seq deque input-seq buffer-depth)))))))


(defn buffered-seq
  "Given an input lazy sequence, realize up to N items ahead but produce
the same sequence.  Defaults to the original seq for buffer-depth of 0."
  [^long buffer-depth input-seq]
  (if (= 0 (max buffer-depth 0))
    input-seq
    (let [deque (ArrayDeque. buffer-depth)]
      (lazy-seq
       (deque-seq deque input-seq buffer-depth)))))


(defn- recur-async-channel-to-lazy-seq
  [to-chan]
  (when-let [item (async/<!! to-chan)]
    (cons item (lazy-seq (recur-async-channel-to-lazy-seq to-chan)))))

(defn async-channel-to-lazy-seq
  "Convert a core-async channel into a lazy sequence where each item
is read via async/<!!.  Sequence ends when channel returns nil."
  [to-chan]
  ;;Avoid reading from to-chan immediately as this could force an immediate
  ;;block where one wasn't expected
  (lazy-seq (recur-async-channel-to-lazy-seq to-chan)))


(utils/export-symbols tech.parallel.next-item-fn
                      create-next-item-fn)


(defn- recur-order-indexed-sequence
  "Given a sequence where we can extract an integer index from each item
in the sequence *and* a priority queue that is ordered on that index
we read from a possibly out of order original sequence and use the priority
queue in order to find the next item."
  [^PriorityQueue order-mechanism original-sequence index-fn next-index]
  (loop [next-item (.peek order-mechanism)
         original-sequence original-sequence]
    (if (and next-item
             (= (long next-index)
                (long (index-fn next-item))))
      (cons (.poll order-mechanism)
            (lazy-seq
             (recur-order-indexed-sequence order-mechanism original-sequence
                                           index-fn (inc (long next-index)))))
      (when-let [insert-item (first original-sequence)]
        (.add order-mechanism insert-item)
        (recur (.peek order-mechanism)
               (rest original-sequence))))))


(defn order-indexed-sequence
  "Given a possibly unordered original sequence and a function that returns indexes
starting from 0 produce an ordered sequence."
  [index-fn original-sequence]
  (let [comp (comparator (fn [a b]
                           (< (long (index-fn a))
                              (long (index-fn b)))))
        order-mechanism (PriorityQueue. 5 comp)]
    (lazy-seq
     (recur-order-indexed-sequence order-mechanism original-sequence
                                   index-fn 0))))

(defn get-default-parallelism
  []
  (+ (.availableProcessors (Runtime/getRuntime)) 2))


(defn- recur-channel-seq->item-seq
  [channel-seq]
  (when-let [next-channel (first channel-seq)]
    (when-let [channel-item (async/<!! next-channel)]
      (cons channel-item
            (lazy-seq (recur-channel-seq->item-seq
                       (rest channel-seq)))))))

(defn channel-seq->item-seq
  "Convert a sequence of channels lazily into a sequence of the first
item read from a given channel."
  [channel-seq]
  (lazy-seq (recur-channel-seq->item-seq channel-seq)))


(defn- wrap-thread-bindings
  [f]
  (let [frame (clojure.lang.Var/cloneThreadBindingFrame)]
    (fn [& args]
      (clojure.lang.Var/resetThreadBindingFrame frame)
      (apply f args))))


(defn queued-sequence
  "Returns a map containing a shutdown function *and* a sequence
  derived from the queue operation:
  {:shutdown-fn
  :sequence}
  Shutting down the sequence is necessary in the case of an infinite
  so you can free the resources associated with this queued sequence.
  When using ordering it does not make any sense to have
  num-threads > queue-depth because we cannot read more than queue-depth ahead
  into the src seq.

  There is an additional invariant that there are never more
  that queue-depth items in flight.  This invariant means there has to be blocking
  on the read-head of the input sequence.

  **When callers dereference the output sequence,
  however, there may at that instant be queue-depth + 1 items in flight.  Callers
  need to be aware of this.**

  A thread initialization function is available in case you have an operation
  that needs to happen exactly once per thread.

  Clients can dictate which executor service to use for the thread pool.  If a service
  is not specified this function internally will allocate a forkjoinpool with num-threads
  parallelism.

  If any errors leak into the processing thread the entire system is immediately halted
  and the error propagated to the calling thread."
  [map-fn map-args & {:keys [queue-depth num-threads thread-init-fn executor-service]
                      :or {queue-depth (get-default-parallelism)
                           num-threads (get-default-parallelism)
                           thread-init-fn nil}}]
  (if (= 0 (max queue-depth 0))
    (apply map map-fn map-args)
    ;;Num threads cannot be more than queue-depth in order to ensure
    ;;the invariant that there are never more than queue-depth items in flight.
    (let [num-threads (long (min num-threads queue-depth))
          primary-sequence (partition (count map-args) (apply interleave map-args))
          read-channels (vec (repeatedly queue-depth #(async/chan 1)))
          ;;infinite sequence of repeated queue-depth channels.  This sequence provides
          ;;the read backpressure mechanism in that every time we realize an output member
          ;;of the sequence we write to a read channel.  This read channel is read queue-depth ahead
          ;;when reading from input on the pool thread thus providing a blocking mechanism.
          read-chan-output-sequence (->> read-channels
                                         (repeat)
                                         (mapcat identity))

          ;;The channels start queue-depth
          read-chan-sequence (concat (repeat queue-depth :no-channel)
                                     read-chan-output-sequence)

          ;;Add in indexes to allow an ordering mechanism
          read-sequence (->> (interleave primary-sequence
                                         read-chan-output-sequence
                                         read-chan-sequence
                                         (range))
                             (partition 4)
                             ;;Read from the read channel to affect blocking
                             (map (fn [[item output-chan seq-read-chan idx]]
                                    (when-not (= seq-read-chan :no-channel)
                                      (async/<!! seq-read-chan))
                                    [item output-chan idx])))

          queue (async/chan queue-depth)
          pool (if executor-service
                 nil
                 (ForkJoinPool. (long num-threads)))
          next-item-fn (create-next-item-fn read-sequence)
          active (atom true)
          shutdown-fn (fn []
                        (reset! active false)
                        ;;Using mapv to force side effects
                        (mapv async/close! read-channels)
                        (async/close! queue))
          map-fn (wrap-thread-bindings map-fn)
          ;;There is a race condition if the sequence is very short because one of the threads
          ;;could trigger shutdown before this function exits.  To avoid that condition, *this*
          ;;function increments the process count and has the decrement-and-shutdown logic
          ;;also.
          process-count (atom (+ num-threads 1))
          dec-process-count (fn []
                              (when (= (swap! process-count dec) 0)
                                (shutdown-fn)
                                (when pool
                                  (.shutdown pool))))
          process-fn (fn []
                       (try
                         (when thread-init-fn
                           (thread-init-fn))
                         (loop [next-read-item (next-item-fn)]
                           (when (and next-read-item @active)
                             (let [[next-item output-chan idx] next-read-item]
                               (async/>!! queue [idx (apply map-fn next-item) output-chan])
                               (recur (next-item-fn)))))
                         (catch Throwable e
                           (async/>!! queue {:queued-sequence-error e})
                           (shutdown-fn)))
                       (dec-process-count))
          ^ExecutorService submit-service (if pool pool executor-service)]
      (try
        ;;convert sequence into output of map-fn while at the same time
        ;;notifying on dereference that we can read the next item in
        ;;from the window if we are using ordering.
        {:sequence (->> (async-channel-to-lazy-seq queue)
                        ;;Catch errors here and rethrow on main thread
                        (map (fn [item]
                               (when-let [^Throwable nested-exception
                                          (:queued-sequence-error item)]
                                 (throw (RuntimeException.
                                         "Error during queued sequence execution:"
                                         nested-exception)))
                               item))
                        (order-indexed-sequence first)
                        (map (fn [[_ output-item output-chan]]
                               ;;Free up the next input item to be read by the thread
                               ;;pool.
                               (async/>!! output-chan 1)
                               output-item)))
         :futures (mapv #(.submit submit-service ^Callable %) (repeat num-threads process-fn))
         :shutdown-fn shutdown-fn}
        (finally
          (dec-process-count))))))


(defn queued-pmap
  "Given a queue depth and a mapping function, run a pmap like operation.

Not for use with infinite sequences as the threads will hang around forever
processing the infinite sequence.  Call queued-sequence directly and use the
shutdown-fn when the infinite sequence isn't necessary any more.

Note that there will possibly be queue-depth + 1 items in flight as
the instant the first output item is dereferenced there is a chance for the
processing threads to grab an item and both will be in flight, adding up to
queue-depth + 1.

A queue depth of zero indicates to use a normal map operation."
  [queue-depth map-fn & args]
  (if (= 0 (max queue-depth 0))
    (apply map map-fn args)
    (:sequence (queued-sequence map-fn args
                                :queue-depth queue-depth))))


(defn create-thread-pool
  "Create a new thread pool.  This is often done as a variable defined
  in a namespace and then used in several thread-pool-pmap function calls.
  The advantage is that multiple pmap sequences can use the same underlying
  thread pool.

  num-threads - Number of threads to create in the thread pool.
  thread-name-fn - Function from pool-index->string thread name"
  ^ForkJoinPool [num-threads thread-name-fn]
  (ForkJoinPool.
   (int num-threads)
   ;;We override the worker factory in order to give our threads good
   ;;names.
   (reify
     ForkJoinPool$ForkJoinWorkerThreadFactory
     (newThread [this pool]
       (let [retval
             (-> (ForkJoinPool/defaultForkJoinWorkerThreadFactory)
                 (.newThread pool))]
         (.setName retval (thread-name-fn (.getPoolIndex retval)))
         retval)))
   nil ;;No special exception handling
   false ;;Async tasks are processed in LIFO
   ))

(defn thread-pool-num-threads
  ^long [^ForkJoinPool thread-pool]
  (long (.getParallelism thread-pool)))


(defn thread-pool-pmap
  "Pmap functionality with preexisting thread pool.  Convenience wrapper
  around using custom thread pools (ExecutorService).
  thread-pool - thread pool to use.
  queue-depth - Queue-depth - how far ahead to allow threads to get.
  map-fn - map fn
  args - sequences"
  [thread-pool queue-depth map-fn & sequences]
  (:sequence (queued-sequence
              map-fn sequences
              :queue-depth queue-depth
              :executor-service thread-pool)))


(defn psink!
  "Terminate a sequence with a side effecting function using n-threads named
  thread-name-{idx}.  !!Items are satisifed in a non-order-dependent fashion!!
  Blocks until execution of the items is complete.
  :n-threads - num thread to use.
  :thread-name - thread name prefix to use.  Is appended with '-{thread-idx}'"
  ([doseq-fn! item-seq {:keys [n-threads
                               thread-name]
                        :or {n-threads 16
                             thread-name "psink-thread"}}]
   (let [thread-pool (create-thread-pool n-threads #(format "%s-%d" thread-name %))
         next-item-fn (create-next-item-fn item-seq)
         process-fn (fn []
                      (try
                        (loop [next-read-item (next-item-fn)]
                          (when next-read-item
                            (try
                              (doseq-fn! next-read-item)
                              (catch Throwable e
                                (log/errorf e "Error processing %s" thread-name)))
                            (recur (next-item-fn))))
                        (catch Throwable e
                          (log/errorf e "Error reading %s" thread-name))))]
     (->> (repeat n-threads process-fn)
          ;;Launch the threads in one distinct step
          (mapv #(.submit thread-pool ^Callable %))
          ;;complete the tasks in another distinct step
          (mapv deref))
     (.shutdown thread-pool)
     :ok))
  ([doseq-fn! item-seq]
   (psink! doseq-fn! item-seq {})))


(utils/export-symbols tech.parallel.for
                      launch-parallel-for)

(defmacro parallel-for
  "Run a side effecting operator over a contiguous set of indexes"
  [idx-var num-iters & body]
  `(pfor/parallel-for ~idx-var ~num-iters ~@body))

(defmacro serial-for
  "Run a side effecting operator over a contiguous set of indexes"
  [idx-var num-iters & body]
  `(pfor/serial-for ~idx-var ~num-iters ~@body))


(utils/export-symbols tech.parallel.require
                      memoize
                      require-resolve)
