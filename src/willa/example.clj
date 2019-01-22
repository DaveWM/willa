(ns willa.example
  (:require [jackdaw.streams :as streams]
            [willa.core :as w]
            [willa.utils :as wu]
            [jackdaw.serdes.edn :as serdes.edn]
            [loom.graph :as l]
            [willa.streams :as ws])
  (:import (org.apache.kafka.streams.kstream JoinWindows)))


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
  [[:topics/input-topic :table]
   [:table :topics/output-topic]])

(def entities
  {:topics/input-topic (assoc input-topic :type :topic)
   :topics/secondary-input-topic (assoc secondary-input-topic :type :topic)
   :topics/tertiary-input-topic (assoc tertiary-input-topic :type :topic)
   :topics/output-topic (assoc output-topic :type :topic)
   :topics/secondary-output-topic (assoc secondary-output-topic :type :topic)
   :stream {:type :kstream
            :xform (map (wu/transform-value (fn [v]
                                              (apply + (remove nil? v)))))}
   :table {:type :ktable
           :group-by (fn [[k v]]
                       [v (count k)])
           :aggregate-adder (fn [acc [k v]]
                              (+ acc v))
           :aggregate-subtractor (fn [acc [k v]]
                                   (- acc v))
           :initial-value 0}})

(def joins
  {[:topics/input-topic :topics/secondary-input-topic :topics/tertiary-input-topic] {:type :inner
                                                                                     :window (JoinWindows/of 10000)}})


(defn start! []
  (doto (streams/kafka-streams (w/build-workflow!
                                 (streams/streams-builder)
                                 {:workflow workflow
                                  :entities entities
                                  :joins joins})
                               app-config)
    (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                    (uncaughtException [_ t e]
                                      (println e))))
    streams/start))

(comment

  (jackdaw.client/subscribe consumer [output-topic])
  (jackdaw.client/seek-to-beginning-eager consumer)
  (map (juxt :key :value) (jackdaw.client/poll consumer 1000))


  (require 'jackdaw.client
           'jackdaw.admin)
  (def admin-client (jackdaw.admin/->AdminClient app-config))
  (jackdaw.admin/create-topics! admin-client topics)
  (jackdaw.admin/list-topics admin-client)
  (def app (start!))

  (jackdaw.admin/delete-topics! admin-client topics)
  (streams/close app)
  (.state app)

  (def producer (jackdaw.client/producer app-config
                                         willa.streams/default-serdes))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic "a" 2))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord secondary-input-topic "key" 2))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord tertiary-input-topic "key" 3))


  (def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                         willa.streams/default-serdes))


  (require 'loom.io)
  (loom.io/view (apply l/digraph workflow))

  (require '[clojure.core.async :as a])
  (defn reset []
    (streams/close app)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/delete-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/create-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (alter-var-root #'app (fn [_] (start!))))

  )
