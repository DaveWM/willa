(defproject willa "0.1.0-SNAPSHOT"
  :description "A Clojure DSL for Kafka Streams"
  :url "http://example.com/FIXME"
  :license {:name "GPL V3"
            :url "https://www.gnu.org/licenses/gpl-3.0.en.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [fundingcircle/jackdaw "0.4.2"]
                 [aysylu/loom "1.0.2"]
                 [rhizome "0.2.9"]]
  :repl-options {:init-ns willa.core})
