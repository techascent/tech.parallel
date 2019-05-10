(ns tech.parallel.next-item-fn)


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
