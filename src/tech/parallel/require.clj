(ns tech.parallel.require
  (:refer-clojure :exclude [memoize]))



(defn memoize
  [memo-fn]
  (let [memo-data (atom {})]
    (fn [& args]
      (locking memo-data
        (get
         (swap! memo-data (fn [arg-val-map]
                            (if (get arg-val-map args)
                              arg-val-map
                              (assoc arg-val-map args
                                     (apply memo-fn args)))))
         args)))))


(def
  ^{:doc
    "Clojure's require is not threadsafe.  So in order to do dynamic require
and resolution of target functions we need to wrap it in a threadsafe
memoizeation of the value.
Usage:
(require-resolve 'clojure.core.async/admix)"}
  require-resolve
  (memoize
   (fn [item-symbol]
     (require (symbol (namespace item-symbol)))
     (resolve item-symbol))))
