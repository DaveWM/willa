(ns willa.example
  (:require [jackdaw.streams :as streams]
            [willa.core :as w]
            [willa.utils :as wu]
            [loom.graph :as l]
            [willa.streams :as ws]
            [willa.workflow :as ww]
            [willa.viz :as wv]
            [willa.experiment :as we])
  (:import (org.apache.kafka.streams.kstream JoinWindows Suppressed Suppressed$BufferConfig TimeWindows)
           (java.time Duration)))


(def app-config
  {"application.id" "willa-test"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})


(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1
          ::w/entity-type :topic}
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
  [[:topics/input-topic :stream]
   [:stream :topics/output-topic]])

(def entities
  (merge {:topics/input-topic input-topic
          :topics/output-topic output-topic
          :stream {::w/entity-type :kstream
                   ::w/xform (comp (map (wu/transform-value inc))
                                   (filter (wu/value-pred even?)))}}))

(def joins {})


(defn start! []
  (let [builder (doto (streams/streams-builder)
                  (w/build-topology! {:workflow workflow
                                      :entities entities
                                      :joins    joins}))]
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
  (def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                         willa.streams/default-serdes))
  (def input-consumer (jackdaw.client/consumer (assoc app-config "group.id" "input-consumer")
                                               willa.streams/default-serdes))
  (jackdaw.client/subscribe consumer [output-topic])
  (jackdaw.client/subscribe input-consumer [input-topic])

  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic "key" 3))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic "key" 2))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord tertiary-input-topic "key" 3))

  (do (jackdaw.client/seek-to-beginning-eager consumer)
      (->> (jackdaw.client/poll consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))

  (do (jackdaw.client/seek-to-beginning-eager input-consumer)
      (->> (jackdaw.client/poll input-consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))

  (wv/view-workflow {:workflow workflow
                     :entities entities
                     :joins joins}
                    {:show-joins true})
  (wv/view-workflow (we/run-experiment {:workflow workflow
                                        :entities entities
                                        :joins joins}
                                       {:topics/input-topic (->> (range 3)
                                                                 (map #(-> {:key "k" :value % :timestamp (* % 100)})))}))


  (defn reset []
    (streams/close app)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/delete-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (jackdaw.admin/create-topics! admin-client topics)
    (a/<!! (a/timeout 100))
    (alter-var-root #'app (fn [_] (start!))))

  (do (jackdaw.client/seek-to-beginning-eager consumer)
      (->> (jackdaw.client/poll consumer 200)
           (map #(select-keys % [:key :value :timestamp]))))
  (->> (we/run-experiment {:workflow workflow
                           :entities entities
                           :joins joins}
                          {:topics/input-topic (do (jackdaw.client/seek-to-beginning-eager input-consumer)
                                                   (->> (jackdaw.client/poll input-consumer 200)
                                                        (map #(select-keys % [:key :value :timestamp]))))})
       :topics/output-topic
       ::we/output)
  )
