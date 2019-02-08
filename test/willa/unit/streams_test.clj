(ns willa.unit.streams-test
  (:require [clojure.test :refer :all]
            [willa.streams :refer :all]
            [jackdaw.streams :as streams]
            [willa.test-utils :as u]
            [willa.utils :as wu])
  (:import (jackdaw.streams.interop CljKStream CljKTable)))


(deftest coerce-to-kstream-test
  (let [builder           (streams/streams-builder)
        mock-topic-config {:topic-name "mock"}]
    (are [input]
      (instance? CljKStream (coerce-to-kstream input))
      (streams/kstream builder mock-topic-config)
      (streams/ktable builder mock-topic-config))))


(deftest coerce-to-ktable-test
  (let [builder           (streams/streams-builder)
        mock-topic-config {:topic-name "mock"}]
    (are [input]
      (instance? CljKTable (coerce-to-ktable input))
      (streams/kstream builder mock-topic-config)
      (streams/ktable builder mock-topic-config))))


(deftest aggregate-test
  (let [builder      (streams/streams-builder)
        input-topic  (u/->topic "input")
        output-topic (u/->topic "output")]

    (-> (streams/kstream builder input-topic)
        (streams/group-by-key)
        (aggregate 0 (fn [acc [k v]] (+ acc v)) identity)
        (streams/to-kstream)
        (streams/to output-topic))

    (is
      (u/results-congruous?
        [:output-topic]

        (u/run-test-machine
          builder
          {:entities {:input-topic input-topic
                      :output-topic (u/->topic "output")}}
          {:input-topic [{:key "k" :value 1 :timestamp 100}
                         {:key "k" :value 2 :timestamp 100}]}
          (fn [journal]
            (= 2 (count (get-in journal [:topics "output"])))))

        {:output-topic [{:key "k" :value 1}
                        {:key "k" :value 3}]}))))


(deftest transduce-stream-test
  (let [builder      (streams/streams-builder)
        input-topic  (u/->topic "input")
        output-topic (u/->topic "output")]

    (-> (streams/kstream builder input-topic)
        (transduce-stream (map (wu/transform-value inc)))
        (streams/to output-topic))

    (is
      (u/results-congruous?
        [:output-topic]

        (u/run-test-machine
          builder
          {:entities {:input-topic input-topic
                      :output-topic (u/->topic "output")}}
          {:input-topic [{:key "k" :value 1 :timestamp 100}]}
          (fn [journal]
            (= 1 (count (get-in journal [:topics "output"])))))

        {:output-topic [{:key "k" :value 2}]}))))
