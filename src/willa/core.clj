(ns willa.core
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn]
            [loom.graph :as l]
            [loom.alg :as lalg]
            rhizome.viz)
  (:import (org.apache.kafka.streams.kstream Transformer TimeWindows JoinWindows KStream)
           (org.apache.kafka.streams.processor ProcessorContext)))

(defn transform-value [f]
  (fn [[k v]] [k (f v)]))
(defn transform-values [f]
  (fn [[k v]] (map vector (repeat k) (f v))))
(defn value-pred [f]
  (fn [[k v]] (f v)))


(def app-config
  {"application.id" "willa-test"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})


(def input-topic
  {:topic-name "input-topic"
   :replication-factor 1
   :partition-count 1
   :key-serde (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})
(def secondary-input-topic
  {:topic-name "secondary-input-topic"
   :replication-factor 1
   :partition-count 1
   :key-serde (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})
(def output-topic
  {:topic-name "output-topic"
   :replication-factor 1
   :partition-count 1
   :key-serde (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})
(def topics
  [input-topic
   secondary-input-topic
   output-topic])

(def workflow
  [[:topics/input-topic :stream]
   [:topics/secondary-input-topic :stream]
   [:stream :topics/output-topic]])

(def entities
  {:topics/input-topic (assoc input-topic :type :topic)
   :topics/secondary-input-topic (assoc secondary-input-topic :type :topic)
   :topics/output-topic (assoc output-topic :type :topic)
   :stream {:type :kstream
            :xform (map (transform-value (fn [v]
                                           (apply + v))))}})

(def joins
  {[:topics/input-topic :topics/secondary-input-topic] {:type :inner
                                                        :window (JoinWindows/of 10000)}})


(def default-serdes
  {:key-serde (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})


(deftype TransducerTransformer [xform ^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
  (transform [_ k v]
    (let [rf (fn
               ([context] context)
               ([^ProcessorContext context [k v]]
                (.forward context k v)
                (.commit context)
                context))]
      (try
        ((xform rf) context [k v])
        (catch Exception e
          (.printStackTrace e))))
    nil)
  (close [_]))


(defn transduce-stream [kstream xform]
  (streams/transform kstream #(TransducerTransformer. xform nil)))


(defmulti ->kstream (fn [builder entity]
                      (:type entity)))

(defmethod ->kstream :topic [builder entity]
  (streams/kstream builder entity))

(defmethod ->kstream :kstream [_ entity]
  (:kstream entity))


(defmulti ->joinable (fn [builder entity]
                       (:type entity)))

(defmethod ->joinable :topic [builder entity]
  (->kstream builder entity))

(defmethod ->joinable :kstream [builder entity]
  (->kstream builder entity))


(defmulti join* (fn [join-config kstream-or-ktable other join-fn]
                  [(:type join-config) (class kstream-or-ktable) (class other)]))
(defmethod join*
  [:inner KStream KStream]
  [join-config kstream other join-fn]
  (streams/join-windowed kstream other join-fn (:window join-config)
                         default-serdes default-serdes))

(defmethod join*
  [:left KStream KStream]
  [join-config kstream other join-fn]
  (streams/left-join-windowed kstream other join-fn (:window join-config)
                              default-serdes default-serdes))

(defmethod join*
  [:outer KStream KStream]
  [join-config kstream other join-fn]
  (streams/outer-join-windowed kstream other join-fn (:window join-config)
                               default-serdes default-serdes))


(defn join
  ([_ entity] entity)
  ([join-config joinable other]
   (join* join-config joinable other (fn [v1 v2] [v1 v2])))
  ([join-config joinable o1 o2 & others]
   (apply join
          join-config
          (join* join-config (join join-config joinable o1) o2 (fn [vs v] (conj vs v)))
          others)))

(defmulti build-entity (fn [entity builder parents entities joins]
                         (:type entity)))


(defmethod build-entity :topic [entity builder parents entities _]
  (doseq [p (map entities parents)]
    (streams/to (->kstream builder p) entity))
  entity)


(defmethod build-entity :kstream [entity builder parents entities joins]
  (let [[join-order join-config] (->> joins
                                      (filter (fn [[k _]] (= (set k) parents)))
                                      first)
        all-records-stream (->> join-order
                                (map (comp (partial ->joinable builder) entities))
                                (apply join join-config))
        kstream            (if-let [xf (:xform entity)]
                             (transduce-stream all-records-stream xf)
                             all-records-stream)]
    (assoc entity :kstream kstream)))


(defn build-workflow [{:keys [workflow entities joins]}]
  (let [builder (streams/streams-builder)
        g       (apply l/digraph workflow)
        nodes   (lalg/topsort g)]
    (->> nodes
         (map (juxt identity (partial l/predecessors g)))
         (reduce (fn [enriched-entities [node parents]]
                   (update enriched-entities node build-entity builder parents enriched-entities joins))
                 entities))
    builder))


(defn start! []
  (doto (streams/kafka-streams (build-workflow {:workflow workflow
                                                :entities entities
                                                :joins joins})
                               app-config)
    (.setUncaughtExceptionHandler (reify Thread$UncaughtExceptionHandler
                                    (uncaughtException [_ t e]
                                      (println e))))
    streams/start))

(comment

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
                                         {:value-serde (serdes.edn/serde)
                                          :key-serde (serdes.edn/serde)}))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic "key" 1337))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord secondary-input-topic "key" 12342))


  (def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                         {:value-serde (serdes.edn/serde)
                                          :key-serde (serdes.edn/serde)}))
  (jackdaw.client/subscribe consumer [output-topic])
  (jackdaw.client/seek-to-beginning-eager consumer)
  (map :value (jackdaw.client/poll consumer 100))
  )
