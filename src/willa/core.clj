(ns willa.core
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn]
            [loom.graph :as l]
            [loom.alg :as lalg]
            rhizome.viz
            [willa.streams :as ws]
            [willa.utils :as wu])
  (:import (org.apache.kafka.streams.kstream Windowed)))


(defmulti entity->kstream (fn [builder entity]
                            (:type entity)))

(defmethod entity->kstream :topic [builder entity]
  (streams/kstream builder entity))

(defmethod entity->kstream :kstream [_ entity]
  (:kstream entity))

(defmethod entity->kstream :ktable [_ entity]
  (-> (ws/coerce-to-kstream (:ktable entity))
      (streams/map (fn [[k v]]
                     [(if (instance? Windowed k) (.key k) k)
                      v]))))


(defmulti entity->ktable (fn [builder entity]
                           (:type entity)))

(defmethod entity->ktable :topic [builder entity]
  (streams/ktable builder entity))

(defmethod entity->ktable :kstream [_ entity]
  (ws/coerce-to-ktable (:kstream entity)))

(defmethod entity->ktable :ktable [_ entity]
  (:ktable entity))


(defmulti ->joinable (fn [builder entity]
                       (:type entity)))

(defmethod ->joinable :topic [builder entity]
  (entity->kstream builder entity))

(defmethod ->joinable :kstream [builder entity]
  (entity->kstream builder entity))

(defmethod ->joinable :ktable [builder entity]
  (entity->ktable builder entity))


(defn get-join [joins parents]
  (->> joins
       (filter (fn [[k _]] (= (set k) parents)))
       first))


(defn join-entities [builder [join-order join-config] entities]
  (->> join-order
       (map (comp (partial ->joinable builder) entities))
       (apply ws/join join-config)))


(defmulti build-entity (fn [entity builder parents entities joins]
                         (:type entity)))


(defmethod build-entity :topic [entity builder parents entities _]
  (doseq [p (map entities parents)]
    (streams/to (entity->kstream builder p) entity))
  entity)


(defmethod build-entity :kstream [entity builder parents entities joins]
  (let [kstream (if (wu/single-elem? parents)
                  (entity->kstream builder (get entities (first parents)))
                  (-> (join-entities builder (get-join joins parents) entities)
                      ws/coerce-to-kstream))]
    (assoc entity :kstream (cond-> kstream
                                   (:xform entity) (ws/transduce-stream (:xform entity))))))


(defmethod build-entity :ktable [entity builder parents entities joins]
  (let [ktable-or-kstream (if (wu/single-elem? parents)
                            (entity->ktable builder (get entities (first parents)))
                            (join-entities builder (get-join joins parents) entities))]
    (assoc entity :ktable (cond-> ktable-or-kstream
                                  (:window-by entity) (streams/to-kstream)
                                  (:group-by entity) (streams/group-by (:group-by entity) ws/default-serdes)
                                  (:window-by entity) (ws/window-by (:window-by entity))
                                  (:aggregate-adder entity) (ws/aggregate (:initial-value entity)
                                                                       (:aggregate-adder entity)
                                                                       (:aggregate-subtractor entity))))))


(defn build-workflow! [builder {:keys [workflow entities joins]}]
  (let [g     (apply l/digraph workflow)
        nodes (lalg/topsort g)]
    (->> nodes
         (map (juxt identity (partial l/predecessors g)))
         (reduce (fn [built-entities [node parents]]
                   (update built-entities node build-entity builder parents built-entities joins))
                 entities))))
