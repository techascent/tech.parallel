(ns think.parallel.core
  (:require [clojure.core.async :as async])
  (:import [java.util.concurrent ForkJoinPool Callable Future]
           [java.util ArrayDeque PriorityQueue Comparator]))


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
the same sequence"
  [^long buffer-depth input-seq]
  (let [deque (ArrayDeque. buffer-depth)]
    (deque-seq deque input-seq buffer-depth)))


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


(defn create-next-item-fn
  "Given a sequence return a function that each time called (with no arguments)
returns the next item in the sequence in a mutable fashion."
  [item-sequence]
  (let [primary-sequence (atom item-sequence)]
    (fn []
      (loop [sequence @primary-sequence]
        (when-let [next-item (first sequence)]
          (if-not (compare-and-set! primary-sequence sequence (rest sequence))
            (recur @primary-sequence)
            next-item))))))


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
that needs to happen exactly once per thread."
  [map-fn map-args & {:keys [queue-depth num-threads thread-init-fn]
                      :or {queue-depth (get-default-parallelism)
                           num-threads (get-default-parallelism)
                           thread-init-fn nil}}]
  (if (= 0 (max queue-depth 0))
    (apply map map-fn map-args)
    ;;Num threads cannot be more than queue-depth in order to ensure
    ;;the invariant that there are never more than queue-depth items in flight.
    (let [num-threads (long (min num-threads queue-depth))
          primary-sequence (partition (count map-args) (apply interleave map-args))
          ;;Number of write channels is equal to the queue depth.
          write-channels (vec (repeatedly queue-depth async/chan))
          ;;infinite sequence of repeated queue-depth channels.  This sequence provides
          ;;our ordering mechanism and our backpressure mechanism
          write-chan-sequence (->> write-channels
                                   (repeat)
                                   (mapcat identity))
          read-sequence (->> (interleave primary-sequence
                                         write-chan-sequence)
                             (partition 2))
          waiting-write-channels (atom #{})
          pool (ForkJoinPool. (long num-threads))
          next-item-fn (create-next-item-fn read-sequence)
          process-count (atom 0)
          active (atom true)
          shutdown-fn (fn []
                        (reset! active false)
                        ;;Using mapv to force side effects
                        (mapv async/close! write-channels)
                        (.shutdown pool))

          process-fn (fn []
                       (swap! process-count inc)
                       (when thread-init-fn
                         (thread-init-fn))

                       ;;The theory here is that as long as there are <= threads
                       ;;than write-channels we can't get more items in flight than
                       ;;queue depth at this level.
                       (loop [next-read-item (next-item-fn)]
                         (when (and next-read-item @active)
                           (let [[next-item write-channel] next-read-item]
                             (try
                               (async/>!! write-channel (apply map-fn next-item))
                               (catch Throwable e
                                 (async/>!! write-channel
                                            {:queued-sequence-error e})
                                 (shutdown-fn)))
                             (recur (next-item-fn)))))

                       (when (= (swap! process-count dec) 0)
                         (shutdown-fn)))]
      ;;Start up all processing threads
      (doseq [thread-idx (range num-threads)]
        (.submit pool ^Callable process-fn))

      ;;convert sequence into output of map-fn while at the same time
      ;;notifying on dereference that we can read the next item in
      ;;from the window if we are using ordering.
      {:sequence (->> (channel-seq->item-seq write-chan-sequence)
                      ;;Catch errors here and rethrow on main thread
                      (map (fn [item]
                             (when-let [^Throwable nested-exception
                                        (:queued-sequence-error item)]
                               (throw (RuntimeException.
                                       "Error during queued sequence execution:"
                                       nested-exception)))
                             item)))
       :shutdown-fn shutdown-fn})))


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



(defn launch-parallel-for
  "Given a function that takes exactly 2 arguments, a start-index and a length,
call this function exactly N times where N is ForkJoinPool/getCommonPoolParallelism.
Indexes will be split as evenly as possible among the invocations."
  [^long num-iters parallel-for-fn]
  (if (< num-iters (* 2 (ForkJoinPool/getCommonPoolParallelism)))
    (parallel-for-fn 0 num-iters)
    (let [num-iters (long num-iters)
          parallelism (ForkJoinPool/getCommonPoolParallelism)
          group-size (quot num-iters parallelism)
          overflow (rem num-iters parallelism)
          overflow-size (+ group-size 1)
          group-count (min num-iters parallelism)
          ;;Get pairs of (start-idx, len) to launch callables
          groups (map (fn [^long callable-idx]
                        (let [group-len (if (< callable-idx overflow)
                                          overflow-size
                                          group-size)
                              group-start (+ (* overflow-size
                                                (min overflow callable-idx))
                                             (* group-size
                                                (max 0 (- callable-idx overflow))))]
                          [group-start group-len]))
                      (range parallelism))
          callables (map (fn [[start-idx len]]
                           (fn [] (parallel-for-fn start-idx len)))
                         groups)
          common-pool (ForkJoinPool/commonPool)
          ;;launch the missiles
          futures (mapv #(.submit common-pool ^Callable %) callables)]
      (doseq [^Future fut futures]
        (.get fut)))))



(defmacro parallel-for
  "Like clojure.core.matrix.macros c-for except this expects index that run from 0 -> num-iters.
Idx is named idx-var and body will be called for each idx in parallel."
  [idx-var num-iters & body]
  `(launch-parallel-for ~num-iters
                        (fn [^long group-start# ^long group-len#]
                          (let [group-end# (+ group-start# group-len#)]
                            (loop [~idx-var group-start#]
                              (when (< ~idx-var group-end#)
                                ~@body
                                (recur (inc ~idx-var))))))))
