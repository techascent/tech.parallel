(ns think.parallel.core
  (:require [clojure.core.async :as async])
  (:import [java.util.concurrent ForkJoinPool Callable Future]))


(defn async-channel-to-lazy-seq
  "Convert a core-async channel into a lazy sequence where each item is read via
async/<!!.  Sequence ends when channel returns nil."
  [to-chan]
  (when-let [item (async/<!! to-chan)]
    (cons item (lazy-seq (async-channel-to-lazy-seq to-chan)))))


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


(defn queued-sequence
  "Returns a map containing a shutdown function *and* a sequence
derived from the queue operation:
{:shutdown-fn
 :sequence}
Even though there is a shutdown function, users still need to drain the rest of the sequence after
calling shutdown.  Does not preserve order of input sequence; this would cause deadlock when
used in combination with a queue of a fixed depth and a naive implementation.  An implementation
that blocks on read as well on write for a given window of data would preserve order while avoiding
deadlock."
  [queue-depth num-proc-threads map-fn & args]
  (let [primary-sequence (partition (count args) (apply interleave args))
        queue (async/chan queue-depth)
        pool (ForkJoinPool. num-proc-threads)
        next-item-fn (create-next-item-fn primary-sequence)
        process-count (atom 0)
        active (atom true)
        process-fn (fn []
                     (swap! process-count inc)
                     (try
                       (loop [next-item (next-item-fn)]
                         (if (and next-item @active)
                           (do
                             (async/>!! queue (apply map-fn next-item))
                             (recur (next-item-fn)))))
                       (catch Throwable e
                         (reset! active false)
                         (async/>!! queue {:queued-pmap-error e})
                         (async/close! queue)))
                     (when (= (swap! process-count dec) 0)
                       (async/close! queue)
                       (.shutdown pool)))
        _ (doseq [idx (range (ForkJoinPool/getCommonPoolParallelism))]
            (.submit pool ^Callable process-fn))
        return-sequence  (map (fn [item]
                                (when-let [^Throwable nested-exception (:queued-pmap-error item)]
                                  (throw (RuntimeException. "Error during queued sequence execution:" nested-exception)))
                                item)
                              (async-channel-to-lazy-seq queue))
        shutdown-fn (fn []
                      (reset! active false)
                      (async/close! queue)
                      (.shutdown pool))]
    {:sequence return-sequence
     :shutdown-fn shutdown-fn}))


(defn queued-pmap
  "Given a queue depth and a mapping function, run a pmap like operation.
This operation does not preserve order, however."
  [queue-depth map-fn & args]
  (:sequence (apply queued-sequence queue-depth 16 map-fn args)))



(defn launch-parallel-for
  "Given a function that takes exactly 2 arguments, a start-index and a length,
call this function exactly N times where N is ForkJoinPool/getCommonPoolParallelism.
Indexes will be split as evenly as possible among the functions."
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
