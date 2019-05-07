(ns tech.parallel.utils)


(defmacro export-symbols
  [src-ns & symbol-list]
  (let [public-map (ns-publics (find-ns src-ns))]
    `(do
       ~@(mapv (fn [sym-name]
                 `(def ~(with-meta (symbol (name sym-name))
                          (let [org-meta (meta (get public-map (symbol (name sym-name))))]
                            {:doc (:doc org-meta)
                             :arglists `(quote ~(:arglists org-meta))
                             :macro (:macro org-meta)}))
                    #'~(symbol (name src-ns) (name sym-name))))
               symbol-list))))
