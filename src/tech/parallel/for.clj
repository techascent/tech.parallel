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
  Indexes will be split as evenly as possible among the invocations.  Uses
  ForkJoinPool/commonPool for parallelism."
  ([^long num-iters parallel-for-fn concat-fn]
   (if (< num-iters (* 2 (ForkJoinPool/getCommonPoolParallelism)))
     (parallel-for-fn 0 num-iters)
     (let [num-iters (long num-iters)
           parallelism (ForkJoinPool/getCommonPoolParallelism)
           group-size (quot num-iters parallelism)
           overflow (rem num-iters parallelism)
           overflow-size (+ group-size 1)
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
                            #(parallel-for-fn start-idx len))
                          groups)
           common-pool (ForkJoinPool/commonPool)
           futures (mapv #(.submit common-pool ^Callable %) callables)]
       (->> futures
            (map #(.get ^Future %))
            concat-fn))))
  ([num-iters parallel-for-fn]
   (launch-parallel-for num-iters parallel-for-fn #(apply concat %))))



(defmacro parallel-for
  "Like clojure.core.matrix.macros c-for except this expects index that run from 0 ->
  num-iters.  Idx is named idx-var and body will be called for each idx in parallel.
  Uses forkjoinpool's common pool for parallelism"
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
                                    (recur (inc ~idx-var))))))
                            ;;throw away result
                            (constantly nil)))))


(defn indexed-pmap
  "Given a function that receives a start-idx and group-len, efficiently run N that
  function over a set of integers return concatentated results.  Uses ForkJoinPool's
  common pool for parallelism."
  ([indexed-pmap-fn num-iters concat-fn]
   (let [num-iters (long num-iters)]
     (if (< num-iters (* 2 (ForkJoinPool/getCommonPoolParallelism)))
       (indexed-pmap-fn 0 num-iters)
       (launch-parallel-for num-iters indexed-pmap-fn concat-fn))))
  ([indexed-pmap-fn num-iters]
   (indexed-pmap indexed-pmap-fn num-iters #(apply concat %))))
