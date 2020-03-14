(defproject techascent/tech.parallel "2.8-SNAPSHOT"
  :description "Library for parallelization primitives"
  :url "http://github.com/tech-ascent/tech.parallel"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :plugins [[lein-tools-deps "0.4.1"]]
  :middleware [lein-tools-deps.plugin/resolve-dependencies-with-deps-edn]
  :lein-tools-deps/config {:config-files [:install :user :project]})
