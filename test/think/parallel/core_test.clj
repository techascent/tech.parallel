(ns think.parallel.core-test
  (:require [clojure.test :refer :all]
            [think.parallel.core :as parallel]))


(deftest basic-queued-sequence
  (let [result (:sequence (parallel/queued-sequence 16 10 inc (range 1000)))]
    (is (= (count (distinct result)) 1000))
    (is (= (vec (sort result)) (vec (range 1 1001))))))


(deftest queued-sequence-with-exception
  (let [result (:sequence (parallel/queued-sequence 16 10 (fn [idx]
                                                            (when (= idx 100)
                                                              (throw (Exception. "Failure!!")))
                                                            (inc idx))
                                                    (range 1000)))]
    (is (thrown? RuntimeException (vec result)))))


(deftest basic-parallel-for
  (let [result-ary (int-array 1000)]
    (parallel/parallel-for idx 1000
                           (aset result-ary idx (inc idx)))
    (is (= (count (distinct (vec result-ary))) 1000))))
