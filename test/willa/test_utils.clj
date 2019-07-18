(ns willa.test-utils
  (:require [clojure.test :refer :all]
            [willa.streams :as ws]
            [jackdaw.streams :as streams]
            [jackdaw.test :as jt]
            [jackdaw.streams.mock :as mock]
            [loom.graph :as l]
            [willa.experiment :as we]
            [willa.core :as w]
            [willa.utils :as wu]
            [willa.workflow :as ww])
  (:import (java.util Properties)
           (org.apache.kafka.streams TopologyTestDriver)))


(defn builder->test-driver
  [builder]
  (let [topology (-> (streams/streams-builder* builder)
                     (.build))]
    (TopologyTestDriver. topology
                         (doto (Properties.)
                           (.put "application.id"      (str (java.util.UUID/randomUUID)))
                           (.put "bootstrap.servers"   "fake")
                           (.put "default.key.serde"   "jackdaw.serdes.EdnSerde")
                           (.put "default.value.serde" "jackdaw.serdes.EdnSerde"))
                         0)))

(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1
          ::w/entity-type :topic}
         ws/default-serdes))


(defn mock-transport [builder topology]
  (let [driver (builder->test-driver builder)]
    (jt/mock-transport {:driver driver}
                       (wu/get-topic-name->metadata (:entities topology)))))


(defn run-test-machine [builder {:keys [entities] :as topology} inputs watch-fn]
  (let [transport    (mock-transport builder topology)]
    (jt/with-test-machine
      transport
      (fn [machine]
        (let [results (jt/run-test
                        machine
                        (concat
                          (->> inputs
                               (mapcat (wu/transform-values identity))
                               (sort-by (wu/value-pred :timestamp))
                               (map (fn [[e r]]
                                      [:write!
                                       (:topic-name (get entities e))
                                       (:value r) {:key (:key r)
                                                   :timestamp (:timestamp r)}])))
                          [[:watch watch-fn]]))]
          (->> (get-in results [:journal :topics])
               (map (wu/transform-key #(->> entities
                                            (filter (fn [[k v]] (= % (:topic-name v))))
                                            (map key)
                                            first)))
               (map (wu/transform-value (partial sort-by :offset)))
               (into {})))))))


(defn run-tm-for-workflow [topology inputs watch-fn]
  (let [builder (doto (streams/streams-builder)
                  (w/build-topology! topology))]
    (run-test-machine builder topology inputs watch-fn)))


(defn exercise-workflow [topology inputs]
  (let [experiment-entities (:entities (we/run-experiment topology inputs))
        output-topic-keys   (wu/leaves (:workflow topology))]
    (let [ttd-results        (run-tm-for-workflow topology inputs
                                                  (fn [journal]
                                                    (->> output-topic-keys
                                                         (every? (fn [t]
                                                                   (= (count (get-in journal [:topics (:topic-name (get experiment-entities t))]))
                                                                      (count (get-in experiment-entities [t ::we/output]))))))))
          experiment-results (->> experiment-entities
                                  (filter (wu/key-pred output-topic-keys))
                                  (map (wu/transform-value #(->> (::we/output %)
                                                                 (sort-by :timestamp))))
                                  (into {}))]
      {:official-results ttd-results
       :experiment-results experiment-results})))


(defn results-congruous? [output-topics & results]
  (let [->comparable (fn [topic->results]
                       (->> (select-keys topic->results output-topics)
                            (map (wu/transform-value (partial map #(select-keys % [:key :value]))))))]
    (->> results
         (map ->comparable)
         (apply =))))
