(defproject thinktopic/think.parallel "0.3.7-SNAPSHOT"
  :description "Library for parallelization primitives"
  :url "http://github.com/thinktopic/think.parallel"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.8.0"]
                 [org.clojure/core.async "0.3.441"]
                 [com.climate/claypoole "1.1.4"]]

  :think/meta {:type :library :tags [:low-level]})
