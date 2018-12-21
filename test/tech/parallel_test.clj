(ns tech.parallel-test
  (:require [clojure.test :refer :all]
            [tech.parallel :as parallel]))


(deftest order-indexed-sequence
  (is (= (vec (range 1000))
         (vec
          (parallel/order-indexed-sequence identity
                                           (shuffle (range 1000)))))))


(deftest ordered-queued-sequence
  (let [result (:sequence (parallel/queued-sequence inc [(range 1000)]
                                                    :ordered? true))]
    (is (= (count (distinct result)) 1000))
    (is (= (vec result) (vec (range 1 1001))))))


(deftest queued-sequence-with-exception
  (let [result (:sequence (parallel/queued-sequence (fn [idx]
                                                      (when (= idx 100)
                                                        (throw (Exception. "Failure!!")))
                                                      (inc idx))
                                                    [(range 1000)]))]
    (is (thrown? RuntimeException (vec result)))))


(deftest queue-pmap-no-queue
  (let [result (vec (parallel/queued-pmap 0 inc (range 100)))]
    (is (= result (vec (range 1 101))))))


(deftest ordered-queued-sequence-items-in-flight
  (let [items-in-flight (atom 0)
        queue-depth 10
        input-sequence (range 1000)
        proc-fn (fn [item]
                  (swap! items-in-flight inc)
                  ;;Add 1 to queue depth because we don't decrement until dereference
                  (when-not (<= @items-in-flight (+ queue-depth 1))
                    (throw (ex-info (format "Items in flight invariant borked: %s"
                                            @items-in-flight)
                                    {:queue-depth queue-depth
                                     :items-in-flight @items-in-flight
                                     :item item})))
                  (inc item))
        result (map (fn [item]
                      (swap! items-in-flight dec)
                      item)
                    (:sequence (parallel/queued-sequence proc-fn [input-sequence]
                                                         :queue-depth 10
                                                         :ordered? true)))]
    (is (= (vec result) (vec (range 1 1001))))))


(deftest ordered-queue-sequence-items-in-flight-2
  (let [queue-depth 5
        ;;Infinite sequence of double arrays but only queue-depth + 1 repeating
        data-arrays (->> (repeatedly (+ queue-depth 1) #(double-array 1))
                         (repeat)
                         (mapcat identity))
        proc-fn (fn [idx ^doubles data-array]
                  (aset data-array 0 (double idx))
                  data-array)
        ;;if the invariant isn't met then we will get duplicates where one of the threads
        ;;overwrites a data array it should not.
        result (map (fn [^doubles data-array]
                      (long (aget data-array 0)))
                    (:sequence (parallel/queued-sequence proc-fn [(range 1000) data-arrays]
                                                         :queue-depth queue-depth
                                                         :ordered? true)))]
        (is (= (vec result) (vec (range 1000))))))


(deftest basic-parallel-for
  (let [result-ary (int-array 1000)]
    (parallel/parallel-for idx 1000
                           (aset result-ary idx (inc idx)))
    (is (= (count (distinct (vec result-ary))) 1000))))

(defmacro or=
  "Is a equal to b or c"
  [a b c]
  `(let [a# ~a]
     (or (= a# ~b)
         (= a# ~c))))

(deftest buffered-seq
  (let [realized-count (atom 0)
        test-seq (->> (parallel/queued-pmap
                       1
                       (fn [idx]
                         (swap! realized-count inc)
                         idx)
                       (range 100))
                      (parallel/buffered-seq 5)
                      (map identity))]
    ;;If the pmap thread has launched then this number is 1.
    ;;If it hasn't yet then this number is 0.  We haven't blocked
    ;;on it yet so it could be legitimately be either 1 or 0.
    ;;This is true for any of the realized counts and it is important
    ;;to understand this ambiguity.
    (is (or= @realized-count 0 1))
    (is (= 0 (first test-seq)))
    (is (or= @realized-count 5 6))
    (is (= 1 (second test-seq)))
    (is (or= @realized-count 6 7))))


(def ^:dynamic *increment-count* 0)

(deftest thread-bindings
  (with-bindings {#'*increment-count* 1}
    (let [increment-atom (atom 0)]
      (->> (range 100)
           (parallel/queued-pmap 10 (fn [_]
                                      (swap! increment-atom (fn [val]
                                                              (+ val *increment-count*)))))
           dorun)
      (is (= @increment-atom 100)))))


(deftest precise-queue-behavior
  (let [access-count (atom 0)
        in-flight-max 2
        check-in-flight-max (fn []
                              (when (> @access-count in-flight-max)
                                (throw (ex-info "In flight max constraint violation"
                                                {:max-val @access-count}))))]
    (->> (range 1000)
         (parallel/queued-pmap (- in-flight-max 1)
                               (fn [_]
                                 (swap! access-count inc)
                                 (Thread/sleep 10)
                                 (check-in-flight-max)))
         (map (fn [_]
                (Thread/sleep 10)
                (check-in-flight-max)
                (swap! access-count dec)))
         vec)
    (is (= @access-count 0))))


(deftest short-sequence-clean-shutdown
  ;;A very short sequence can result in an error if there are more threads than potential processors and
  ;;the launch sequence doesn't finish before one of the threads shuts down the sequence.
  (let [error
        (try
          (dotimes [iter 10]
            (->> (parallel/queued-pmap 100 identity [1 2 3])
                 doall)
            nil)
          (catch Throwable e e))]
    (is (nil? error))))


(deftest short-sequence-error-clean-shutdown
  ;;A very short sequence can result in an error if there are more threads than potential processors and
  ;;the launch sequence doesn't finish before one of the threads causes an exception.

  (dotimes [iter 20]
    (try
      (->> (parallel/queued-pmap 100 (fn [& args]
                                       (throw (ex-info "Sequence failure" {})))
                                 [1 2 3])
          doall)
      (throw (ex-info "should not make it here"))
      (catch Throwable e
        (is (not (instance? java.util.concurrent.RejectedExecutionException e)))
        nil))))


(deftest parallel-memoize
  (let [test-atom (atom 0)]
    (dotimes [iter 100]
      (let [inc-fn (parallel/memoize #(swap! test-atom inc))]
        (->> (range 20)
             (pmap (fn [ignored]
                     (inc-fn))))))
    (is (= @test-atom 100))))
