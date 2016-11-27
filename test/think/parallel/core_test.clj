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


(deftest basic-parallel-for
  (let [result-ary (int-array 1000)]
    (parallel/parallel-for idx 1000
                           (aset result-ary idx (inc idx)))
    (is (= (count (distinct (vec result-ary))) 1000))))
