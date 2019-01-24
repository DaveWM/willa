(ns willa.example
  (:require [jackdaw.streams :as streams]
            [willa.core :as w]
            [willa.utils :as wu]
            [loom.graph :as l]
            [willa.streams :as ws]
            [willa.workflow :as ww]
            [willa.viz :as wv])
  (:import (org.apache.kafka.streams.kstream JoinWindows Suppressed Suppressed$BufferConfig TimeWindows)
           (java.time Duration)))


(def app-config
  {"application.id" "willa-test"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})


(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1}
         ws/default-serdes))

(def input-topic
  (->topic "input-topic"))
(def secondary-input-topic
  (->topic "secondary-input-topic"))
(def tertiary-input-topic
  (->topic "tertiary-input-topic"))
(def output-topic
  (->topic "output-topic"))
(def secondary-output-topic
  (->topic "secondary-output-topic"))

(def topics
  [input-topic
   secondary-input-topic
   tertiary-input-topic
   output-topic
   secondary-output-topic])


(def workflow
  (concat
    [[:topics/input-topic :stream]
     [:topics/secondary-input-topic :stream]]
    (ww/with-dedupe "stream-dedupe" :stream :topics/output-topic)))

(def entities
  (merge {:topics/input-topic (assoc input-topic ::w/entity-type :topic)
          :topics/secondary-input-topic (assoc secondary-input-topic ::w/entity-type :topic)
          :topics/tertiary-input-topic (assoc tertiary-input-topic ::w/entity-type :topic)
          :topics/output-topic (assoc output-topic ::w/entity-type :topic)
          :topics/secondary-output-topic (assoc secondary-output-topic ::w/entity-type :topic)
          :stream {::w/entity-type :kstream
                   ::w/xform (map (wu/transform-value inc))}}
         (ww/dedupe-entities "stream-dedupe")
         {:suppressed-table {::w/entity-type :ktable
                             ::w/group-by-fn (fn [[k v]] k)
                             ::w/aggregate-initial-value 0
                             ::w/aggregate-adder-fn (fn [acc [k v]]
                                                      (+ acc v))
                             ::w/window (.grace (TimeWindows/of 10000) (Duration/ofMillis 10000))
                             ::w/suppression (Suppressed/untilWindowCloses (Suppressed$BufferConfig/unbounded))}}))

(def joins
  {[:topics/input-topic :topics/secondary-input-topic] {::w/join-type :merge
                                                        ::w/window (JoinWindows/of 10000)}})


(defn start! []
  (let [builder        (streams/streams-builder)
        built-entities (w/build-workflow!
                         builder
                         {:workflow workflow
                          :entities entities
                          :joins joins})]
    (doto (streams/kafka-streams builder
                                 app-config)
      (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                      (uncaughtException [_ t e]
                                        (println e))))
      streams/start)))

(comment

  (require 'jackdaw.client
           'jackdaw.admin
           '[clojure.core.async :as a])

  (def admin-client (jackdaw.admin/->AdminClient app-config))
  (jackdaw.admin/create-topics! admin-client topics)
  (jackdaw.admin/list-topics admin-client)
  (def app (start!))

  (jackdaw.admin/delete-topics! admin-client topics)
  (streams/close app)
  (.state app)

  (def producer (jackdaw.client/producer app-config
                                         willa.streams/default-serdes))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic "key" 3))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord secondary-input-topic "key" 2))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord tertiary-input-topic "key" 3))


  (def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                         willa.streams/default-serdes))
  (jackdaw.client/subscribe consumer [output-topic])
  (do (jackdaw.client/seek-to-beginning-eager consumer)
      (map (juxt :key :value) (jackdaw.client/poll consumer 200)))

  (wv/view-workflow {:workflow workflow
                     :entities entities
                     :joins joins}
                    {:show-joins false})

  (defn reset []
    (streams/close app)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/delete-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/create-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (alter-var-root #'app (fn [_] (start!))))

  )
