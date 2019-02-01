(ns willa.experiment-core-congruity-test
  (:require [clojure.test :refer :all]
            [willa.core :as w]
            [willa.experiment :as we]
            [jackdaw.streams :as streams]
            [willa.streams :as ws]
            [jackdaw.test :as jt]
            [willa.utils :as wu])
  (:import (java.util Properties)
           (org.apache.kafka.streams TopologyTestDriver)
           (org.apache.kafka.streams.kstream JoinWindows)))


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


(defn congruous? [world inputs]
  (let [experiment-entities (:entities (we/run-experiment world inputs))
        builder             (streams/streams-builder)
        built-entities      (w/build-workflow! builder world)]
    (with-open [driver (test-driver builder {"application.id" "test"
                                             "bootstrap.servers" "localhost:9092"
                                             "cache.max.bytes.buffering" "0"})
                tm     (jt/test-machine (jt/mock-transport {:driver driver}
                                                           (->> (:entities world)
                                                                (filter (fn [[k v]]
                                                                          (= :topic (::w/entity-type v))))
                                                                (map (fn [[_ t]] [(:topic-name t) t]))
                                                                (into {}))))]
      (let [results               (jt/run-test
                                    tm
                                    (vec (concat
                                           (->> inputs
                                                (mapcat (fn [[e rs]]
                                                          (map #(-> [e %]) rs)))
                                                (sort-by (fn [[e r]] (:timestamp r)))
                                                (map (fn [[e r]]
                                                       [:write!
                                                        (:topic-name (get-in world [:entities e]))
                                                        (:value r) {:key (:key r)
                                                                    :timestamp (:timestamp r)}])))
                                           [[:watch (fn [journal]
                                                      (= (count (get-in journal [:topics "output-topic"]))
                                                         (count (get-in experiment-entities [:output-topic ::we/output]))))]])))
            test-topology-results (->> (get-in results [:journal :topics])
                                       (mapcat (fn [[topic-name rs]]
                                                 (let [topic-key (->> (:entities world)
                                                                      (filter (fn [[k v]] (= topic-name (:topic-name v))))
                                                                      (map key)
                                                                      first)]
                                                   (->> rs
                                                        (sort-by :offset)
                                                        (map #(select-keys % [:key :value]))
                                                        (map #(-> [topic-key %]))))))
                                       (remove (fn [[topic-key rs]]
                                                 (empty? rs))))
            topic-names (->> (get-in results [:journal :topics])
                             (map key)
                             set)
            experiment-results (->> experiment-entities
                                    (filter (fn [[k v]]
                                              (topic-names (:topic-name v))))
                                    (mapcat (fn [[topic-key entity]]
                                              (->> (::we/output entity)
                                                   (sort-by :timestamp)
                                                   (map #(select-keys % [:key :value]))
                                                   (map #(-> [topic-key %]))))))]
        (= test-topology-results experiment-results)))))


(deftest basic-topology-tests

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :output-topic (->topic "output-topic")
                              :stream {::w/entity-type :kstream}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :output-topic (->topic "output-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (wu/transform-value inc))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:stream :secondary-stream]
                              [:secondary-stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :output-topic (->topic "output-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (wu/transform-value inc))}
                              :secondary-stream {::w/entity-type :kstream
                                                 ::w/xform (filter (wu/value-pred even?))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 50}]}))

  (is (congruous? {:workflow [[:input-topic :table]
                              [:table :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :output-topic (->topic "output-topic")
                              :table {::w/entity-type :ktable}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 50}]})))


(deftest joins-tests

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :secondary-input-topic (->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                  ::w/window (JoinWindows/of 100)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 100}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 150}]}))


  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :secondary-input-topic (->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                  ::w/window (JoinWindows/of 100)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 150}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 100}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :secondary-input-topic (->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :inner
                                                                  ::w/window (JoinWindows/of 100)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 100}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 150}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (->topic "input-topic")
                              :secondary-input-topic (->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                  ::w/window (JoinWindows/of 10)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 150}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 100}]})))
