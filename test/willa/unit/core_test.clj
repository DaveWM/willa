(ns willa.unit.core-test
  (:require [clojure.test :refer :all]
            [willa.core :refer :all]
            [willa.test-utils :as u]
            [willa.utils :as wu]))


(deftest simple-topology-tests

  (is (u/results-congruous?
        [:output-topic]
        (u/run-tm-for-workflow {:workflow [[:input-topic :output-topic]]
                                :entities {:input-topic (u/->topic "input")
                                           :output-topic (u/->topic "output")}}
                               {:input-topic [{:key "k" :value 1 :timestamp 100}]}
                               (fn [journal]
                                 (= 1 (count (get-in journal [:topics "output"])))))

        {:output-topic [{:key "k" :value 1}]}))

  (is (u/results-congruous?
        [:output-topic]
        (u/run-tm-for-workflow {:workflow [[:input-topic :stream]
                                           [:stream :output-topic]]
                                :entities {:input-topic (u/->topic "input")
                                           :output-topic (u/->topic "output")
                                           :stream {:willa.core/entity-type :kstream
                                                    :willa.core/xform (map (wu/transform-value inc))}}}
                               {:input-topic [{:key "k" :value 1 :timestamp 100}]}
                               (fn [journal]
                                 (= 1 (count (get-in journal [:topics "output"])))))

        {:output-topic [{:key "k" :value 2}]}))

  (is (u/results-congruous?
        [:output-topic]
        (u/run-tm-for-workflow {:workflow [[:input-topic :stream1]
                                           [:stream1 :stream2]
                                           [:stream2 :output-topic]]
                                :entities {:input-topic (u/->topic "input")
                                           :output-topic (u/->topic "output")
                                           :stream1 {:willa.core/entity-type :kstream
                                                     :willa.core/xform (map (wu/transform-value inc))}
                                           :stream2 {:willa.core/entity-type :kstream
                                                     :willa.core/xform (filter (wu/value-pred even?))}}}
                               {:input-topic [{:key "k" :value 2 :timestamp 100}
                                              {:key "k" :value 3 :timestamp 150}]}
                               (fn [journal]
                                 (= 1 (count (get-in journal [:topics "output"])))))

        {:output-topic [{:key "k" :value 4}]}))

  (is (u/results-congruous?
        [:output-topic :secondary-output-topic]
        (u/run-tm-for-workflow {:workflow [[:input-topic :stream]
                                           [:stream :output-topic]
                                           [:stream :secondary-output-topic]]
                                :entities {:input-topic (u/->topic "input")
                                           :output-topic (u/->topic "output")
                                           :secondary-output-topic (u/->topic "secondary-output")
                                           :stream {:willa.core/entity-type :kstream
                                                    :willa.core/xform (map (wu/transform-value inc))}}}
                               {:input-topic [{:key "k" :value 1 :timestamp 100}]}
                               (fn [journal]
                                 (and (= 1 (count (get-in journal [:topics "output"])))
                                      (= 1 (count (get-in journal [:topics "secondary-output"]))))))

        {:output-topic [{:key "k" :value 2}]
         :secondary-output-topic [{:key "k" :value 2}]}))

  (is (u/results-congruous?
        [:output-topic]
        (u/run-tm-for-workflow {:workflow [[:input-topic :table]
                                           [:table :output-topic]]
                                :entities {:input-topic (u/->topic "input")
                                           :output-topic (u/->topic "output")
                                           :table {:willa.core/entity-type :ktable}}}
                               {:input-topic [{:key "k" :value 1 :timestamp 100}]}
                               (fn [journal]
                                 (= 1 (count (get-in journal [:topics "output"])))))

        {:output-topic [{:key "k" :value 1}]})))
