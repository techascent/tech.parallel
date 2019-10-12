(ns tech.parallel.utils)


(defmacro export-symbols
  [src-ns & symbol-list]
  (let [public-map (ns-publics (find-ns src-ns))]
    `(do
       ~@(mapv
          (fn [sym-name]
            (let [org-meta (meta (get public-map
                                      (symbol (name sym-name))))]
              `(do
                 (def ~(with-meta (symbol (name sym-name))
                         (let []
                           (when (:macro org-meta)
                             (throw
                              (ex-info
                               (format "Cannot export macros as this breaks aot: %s"
                                       sym-name)
                               {:symbol sym-name})))
                           {:doc (:doc org-meta)
                            :arglists `(quote ~(:arglists org-meta))
                            :source-map (select-keys org-meta [:file :line :column])}))
                   #'~(symbol (name src-ns) (name sym-name)))
                 (alter-meta! #'~(symbol (name sym-name))
                              merge
                              ~(select-keys org-meta [:file :line :column])))))
               symbol-list))))
