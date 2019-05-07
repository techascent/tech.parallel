(ns tech.parallel.for
  (:import [java.util.concurrent ForkJoinPool Callable Future ExecutorService]
           [java.util ArrayDeque PriorityQueue Comparator]))


(defmacro serial-for
  [idx-var num-iters & body]
  `(let [num-iters# (long ~num-iters)]
     (loop [~idx-var 0]
       (when (< ~idx-var num-iters#)
         (do
           ~@body)
         (recur (inc ~idx-var))))))


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
  "Like clojure.core.matrix.macros c-for except this expects index that run from 0 ->
  num-iters.  Idx is named idx-var and body will be called for each idx in parallel."
  [idx-var num-iters & body]
  `(let [num-iters# (long ~num-iters)]
     (if (< num-iters# (* 2 (ForkJoinPool/getCommonPoolParallelism)))
       (serial-for ~idx-var num-iters# ~@body)
       (launch-parallel-for num-iters#
                            (fn [^long group-start# ^long group-len#]
                              (let [group-end# (+ group-start# group-len#)]
                                (loop [~idx-var group-start#]
                                  (when (< ~idx-var group-end#)
                                    ~@body
                                    (recur (inc ~idx-var))))))))))
