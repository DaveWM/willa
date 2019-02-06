(ns willa.test-utils
  (:require [clojure.test :refer :all]
            [willa.streams :as ws]
            [jackdaw.streams :as streams]
            [jackdaw.test :as jt]
            [loom.graph :as l]
            [willa.experiment :as we]
            [willa.core :as w]
            [willa.utils :as wu]
            [willa.workflow :as ww])
  (:import (java.util Properties)
           (org.apache.kafka.streams TopologyTestDriver)))


(defn test-driver
  [builder app-config]
  (let [topology (-> (streams/streams-builder* builder)
                     (.build))]
    (TopologyTestDriver. topology
                         (let [props (Properties.)]
                           (doseq [[k v] app-config]
                             (.setProperty props k v))
                           props)
                         0)))


(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1
          ::w/entity-type :topic}
         ws/default-serdes))


(defn leaves [workflow]
  (let [g (apply l/digraph workflow)]
    (->> g
         (l/nodes)
         (filter #(empty? (l/successors g %)))
         set)))

(defn mock-transport [builder config world]
  (let [driver (test-driver builder config)]
    (-> (jt/mock-transport {:driver driver}
                           (ww/get-topic-name->metadata (:entities world)))
        (update :exit-hooks conj (fn [] (.close driver))))))


(defmacro with-test-machine [name transport & body]
  `(with-open [~name (jt/test-machine ~transport)]
     ~@body))


(defn exercise-workflow [world inputs]
  (let [experiment-entities (:entities (we/run-experiment world inputs))
        builder             (doto (streams/streams-builder)
                              (w/build-workflow! world))
        output-topic-keys   (leaves (:workflow world))
        kafka-config        {"application.id" "test"
                             "bootstrap.servers" "localhost:9092"
                             "cache.max.bytes.buffering" "0"
                             "group.key" "test-group"}
        transport           (mock-transport builder kafka-config world)]
    (with-test-machine tm transport
      (let [results               (jt/run-test
                                    tm
                                    (concat
                                      (->> inputs
                                           (mapcat (fn [[e rs]]
                                                     (map vector (repeat e) rs)))
                                           (sort-by (wu/value-pred :timestamp))
                                           (map (fn [[e r]]
                                                  [:write!
                                                   (:topic-name (get experiment-entities e))
                                                   (:value r) {:key (:key r)
                                                               :timestamp (:timestamp r)}])))
                                      [[:watch (fn [journal]
                                                 (->> output-topic-keys
                                                      (every? (fn [t]
                                                                (= (count (get-in journal [:topics (:topic-name (get experiment-entities t))]))
                                                                   (count (get-in experiment-entities [t ::we/output])))))))]]))
            test-topology-results (->> (get-in results [:journal :topics])
                                       (map (wu/transform-key #(->> (:entities world)
                                                                    (filter (fn [[k v]] (= % (:topic-name v))))
                                                                    (map key)
                                                                    first)))
                                       (map (wu/transform-value (partial sort-by :offset)))
                                       (filter (wu/key-pred output-topic-keys))
                                       (into {}))
            experiment-results    (->> experiment-entities
                                       (filter (wu/key-pred output-topic-keys))
                                       (map (wu/transform-value #(->> (::we/output %)
                                                                      (sort-by :timestamp))))
                                       (into {}))]
        {:official-results test-topology-results
         :experiment-results experiment-results}))))


(defn compare-results [output-topics & results]
  (let [->comparable (fn [topic->results]
                       (->> (select-keys topic->results output-topics)
                            (map (wu/transform-value (partial map #(select-keys % [:key :value]))))))]
    (->> results
         (map ->comparable)
         (apply =))))
