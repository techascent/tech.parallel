(ns think.parallel.core-test
  (:require [clojure.test :refer :all]
            [think.parallel.core :as parallel]))


(deftest order-indexed-sequence
  (is (= (vec (range 1000))
         (vec
          (parallel/order-indexed-sequence identity
                                           (shuffle (range 1000)))))))


(deftest basic-queued-sequence
  (let [result (:sequence (parallel/queued-sequence inc [(range 1000)]
                                                    :ordered? false))]
    (is (= (count (distinct result)) 1000))
    (is (= (sort (vec result)) (vec (range 1 1001))))))


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
