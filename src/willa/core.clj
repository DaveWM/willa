(ns willa.core
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn]
            [loom.graph :as l]
            [loom.alg :as lalg]
            rhizome.viz)
  (:import (org.apache.kafka.streams.kstream Transformer TimeWindows JoinWindows KStream SessionWindows Windowed)
           (org.apache.kafka.streams.processor ProcessorContext)
           (jackdaw.streams.interop CljKStream CljKTable CljKGroupedTable CljKGroupedStream CljTimeWindowedKStream CljSessionWindowedKStream)))

(defn transform-value [f]
  (fn [[k v]] [k (f v)]))
(defn transform-values [f]
  (fn [[k v]] (map vector (repeat k) (f v))))
(defn value-pred [f]
  (fn [[k v]] (f v)))

(defn single-elem? [xs]
  (= (count xs) 1))

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
(def tertiary-input-topic
  {:topic-name "tertiary-input-topic"
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
(def secondary-output-topic
  {:topic-name "secondary-output-topic"
   :replication-factor 1
   :partition-count 1
   :key-serde (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})
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
            :xform (map (transform-value (fn [v]
                                           (apply + (remove nil? v)))))}
   :table {:type :ktable
           :group-by (fn [[k v]]
                       k)
           :aggregate-adder (fn [acc [k v]]
                              (+ acc v))
           :aggregate-subtractor (fn [acc [k v]]
                                   (- acc v))
           :initial-value 0}})

(def joins
  {[:topics/input-topic :topics/secondary-input-topic :topics/tertiary-input-topic] {:type :inner
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
      ((xform rf) context [k v]))
    nil)
  (close [_]))


(defn transduce-stream [kstream xform]
  (streams/transform kstream #(TransducerTransformer. xform nil)))

(defn window-by [kgroupedstream window]
  (cond
    (instance? SessionWindows window) (streams/window-by-session kgroupedstream window)
    (instance? TimeWindows window) (streams/window-by-time kgroupedstream window)))


(defn aggregate [aggregatable initial-value adder-fn subtractor-fn]
  (let [topic-config (merge {:topic-name (str (gensym))}
                            default-serdes)]
    (if (instance? CljKGroupedTable aggregatable)
      (streams/aggregate
        aggregatable
        (constantly initial-value)
        adder-fn
        subtractor-fn
        topic-config)
      (streams/aggregate
        aggregatable
        (constantly initial-value)
        adder-fn
        topic-config))))

(defn *group-by [kstream-or-ktable group-by-fn]
  (if (instance? CljKStream kstream-or-ktable)
    (streams/group-by kstream-or-ktable group-by-fn default-serdes)
    (streams/group-by kstream-or-ktable (fn [[k v]] [(group-by-fn [k v]) v]) default-serdes)))


(defmulti coerce-to-kstream (fn [x]
                              (class x)))
(defmethod coerce-to-kstream CljKTable [ktable]
  (streams/to-kstream ktable))
(defmethod coerce-to-kstream CljKStream [kstream]
  kstream)

(defmulti coerce-to-ktable (fn [x]
                             (class x)))
(defmethod coerce-to-ktable CljKTable [ktable]
  ktable)
(defmethod coerce-to-ktable CljKStream [kstream]
  (-> kstream
      (streams/group-by-key)
      (streams/reduce (fn [_ x] x) (merge {:topic-name (str (gensym))}
                                          default-serdes))))


(defmulti entity->kstream (fn [builder entity]
                            (:type entity)))

(defmethod entity->kstream :topic [builder entity]
  (streams/kstream builder entity))

(defmethod entity->kstream :kstream [_ entity]
  (:kstream entity))

(defmethod entity->kstream :ktable [_ entity]
  (-> (coerce-to-kstream (:ktable entity))
      (streams/map (fn [[k v]]
                     [(if (instance? Windowed k) (.key k) k)
                      v]))))


(defmulti entity->ktable (fn [builder entity]
                           (:type entity)))

(defmethod entity->ktable :topic [builder entity]
  (streams/ktable builder entity))

(defmethod entity->ktable :kstream [_ entity]
  (coerce-to-ktable (:kstream entity)))

(defmethod entity->ktable :ktable [_ entity]
  (:ktable entity))


(defmulti ->joinable (fn [builder entity]
                       (:type entity)))

(defmethod ->joinable :topic [builder entity]
  (entity->kstream builder entity))

(defmethod ->joinable :kstream [builder entity]
  (entity->kstream builder entity))

(defmethod ->joinable :ktable [_ entity]
  (:ktable entity))


(defmulti join* (fn [join-config kstream-or-ktable other join-fn]
                  [(:type join-config) (class kstream-or-ktable) (class other)]))
(defmethod join*
  [:inner CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/join-windowed kstream other join-fn (:window join-config)
                         default-serdes default-serdes))

(defmethod join*
  [:left CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/left-join-windowed kstream other join-fn (:window join-config)
                              default-serdes default-serdes))

(defmethod join*
  [:outer CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/outer-join-windowed kstream other join-fn (:window join-config)
                               default-serdes default-serdes))

(defmethod join*
  [:inner CljKTable CljKTable]
  [_ ktable other join-fn]
  (streams/join ktable other join-fn))

(defmethod join*
  [:left CljKTable CljKTable]
  [_ ktable other join-fn]
  (streams/left-join ktable other join-fn default-serdes default-serdes))

(defmethod join*
  [:outer CljKTable CljKTable]
  [_ ktable other join-fn]
  (streams/outer-join ktable other join-fn))

(defmethod join*
  [:outer CljKStream CljKTable]
  [_ kstream ktable join-fn]
  (streams/left-join kstream ktable join-fn default-serdes default-serdes))


(defn join
  ([_ joinable] joinable)
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
    (streams/to (entity->kstream builder p) entity))
  entity)


(defmethod build-entity :kstream [entity builder parents entities joins]
  (let [[join-order join-config] (->> joins
                                      (filter (fn [[k _]] (= (set k) parents)))
                                      first)
        kstream (if (single-elem? parents)
                  (entity->ktable builder (get entities (first parents)))
                  (->> join-order
                       (map (comp (partial ->joinable builder) entities))
                       (apply join join-config)
                       coerce-to-kstream))]
    (assoc entity :kstream (cond-> kstream
                                   (:xform entity) (transduce-stream (:xform entity))))))


(defmethod build-entity :ktable [entity builder parents entities joins]
  (let [[join-order join-config] (->> joins
                                      (filter (fn [[k _]] (= (set k) parents)))
                                      first)
        ktable-or-kstream (if (single-elem? parents)
                            (entity->ktable builder (get entities (first parents)))
                            (->> join-order
                                 (map (comp (partial ->joinable builder) entities))
                                 (apply join join-config)))]
    (assoc entity :ktable (cond-> ktable-or-kstream
                                  (:window-by entity) (streams/to-kstream)
                                  (:group-by entity) (*group-by (:group-by entity))
                                  (:window-by entity) (window-by (:window-by entity))
                                  (:aggregate-adder entity) (aggregate (:initial-value entity)
                                                                       (:aggregate-adder entity)
                                                                       (:aggregate-subtractor entity))))))


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
                                         default-serdes))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord input-topic "test2" 2))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord secondary-input-topic "key" 2))
  @(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord tertiary-input-topic "key" 3))


  (def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                         default-serdes))
  (jackdaw.client/subscribe consumer [output-topic])
  (jackdaw.client/seek-to-beginning-eager consumer)
  (map :value (jackdaw.client/poll consumer 1000))


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
