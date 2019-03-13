(ns willa.unit.experiment-and-ttd-congruity-test
  (:require [clojure.test :refer :all]
            [willa.core :as w]
            [willa.utils :as wu]
            [willa.test-utils :as u])
  (:import (org.apache.kafka.streams.kstream JoinWindows TimeWindows)))


(defn congruous? [topology inputs]
  (let [{:keys [official-results experiment-results]} (u/exercise-workflow topology inputs)
        output-topics (wu/leaves (:workflow topology))]
    (u/results-congruous? output-topics official-results experiment-results)))


(deftest basic-topology-tests

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :stream {::w/entity-type :kstream}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (wu/transform-value inc))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:stream :secondary-stream]
                              [:secondary-stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (wu/transform-value inc))}
                              :secondary-stream {::w/entity-type :kstream
                                                 ::w/xform (filter (wu/value-pred even?))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 50}]}))

  (is (congruous? {:workflow [[:input-topic :table]
                              [:table :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :table {::w/entity-type :ktable}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 50}]})))


(deftest aggregation-tests

  (is (congruous? {:workflow [[:input-topic :table]
                              [:table :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :table {::w/entity-type :ktable
                                      ::w/window (TimeWindows/of 100)
                                      ::w/group-by-fn (fn [[k v]] k)
                                      ::w/aggregate-initial-value 0
                                      ::w/aggregate-adder-fn (fn [acc [k v]]
                                                               (+ acc v))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 50}]}))

  (is (congruous? {:workflow [[:input-topic :table]
                              [:table :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :table {::w/entity-type :ktable
                                      ::w/window (TimeWindows/of 100)
                                      ::w/group-by-fn (fn [[k v]] k)
                                      ::w/aggregate-initial-value 0
                                      ::w/aggregate-adder-fn (fn [acc [k v]]
                                                               (+ acc v))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 500}]}))

  (is (congruous? {:workflow [[:input-topic :table]
                              [:table :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :output-topic (u/->topic "output-topic")
                              :table {::w/entity-type :ktable
                                      ::w/group-by-fn (fn [[k v]] v)
                                      ::w/aggregate-initial-value 0
                                      ::w/aggregate-adder-fn (fn [acc [k v]]
                                                               (+ acc v))}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 0}
                                 {:key "k" :value 2 :timestamp 500}]})))


(deftest joins-tests

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :secondary-input-topic (u/->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (u/->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                  ::w/window (JoinWindows/of 100)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 100}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 150}]}))


  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :secondary-input-topic (u/->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (u/->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                  ::w/window (JoinWindows/of 100)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 150}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 100}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :secondary-input-topic (u/->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (u/->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :inner
                                                                  ::w/window (JoinWindows/of 100)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 100}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 150}]}))

  (is (congruous? {:workflow [[:input-topic :stream]
                              [:secondary-input-topic :stream]
                              [:stream :output-topic]]
                   :entities {:input-topic (u/->topic "input-topic")
                              :secondary-input-topic (u/->topic "secondary-input-topic")
                              :stream {::w/entity-type :kstream
                                       ::w/xform (map (fn [[k v]]
                                                        [k (apply + (remove nil? v))]))}
                              :output-topic (u/->topic "output-topic")}
                   :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                  ::w/window (JoinWindows/of 10)}}}
                  {:input-topic [{:key "k" :value 1 :timestamp 150}]
                   :secondary-input-topic [{:key "k" :value 2 :timestamp 100}]})))
