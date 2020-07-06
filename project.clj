(defproject willa "0.1.4"
  :description "A Clojure DSL for Kafka Streams"
  :url "http://example.com/FIXME"
  :license {:name "GPL V3"
            :url "https://www.gnu.org/licenses/gpl-3.0.en.html"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [fundingcircle/jackdaw "0.6.6"]
                 [aysylu/loom "1.0.2"]
                 [rhizome "0.2.9"]
                 [org.clojure/math.combinatorics "0.1.4"]]
  :repl-options {:init-ns willa.core}
  :repositories [["confluent" "https://packages.confluent.io/maven/"]
                 ["clojars" "https://clojars.org/repo/"]]
  :profiles {:test {:dependencies [[org.apache.kafka/kafka-streams-test-utils "2.3.0"]
                                   [log4j/log4j "1.2.17"]]}
             :dev {:dependencies [[org.clojure/test.check "0.9.0"]]}
             :ci {:jvm-opts ["-Djava.awt.headless=true"]}})
