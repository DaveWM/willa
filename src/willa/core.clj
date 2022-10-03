(ns willa.core
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn]
            [loom.graph :as l]
            [loom.alg :as lalg]
            [willa.streams :as ws]
            [willa.utils :as wu])
  (:import (org.apache.kafka.streams.kstream Windowed)))


(defmulti entity->kstream (fn [builder entity]
                            (::entity-type entity)))

(defmethod entity->kstream :topic [builder entity]
  (streams/kstream builder entity))

(defmethod entity->kstream :kstream [_ entity]
  (::kstreams-object entity))

(defmethod entity->kstream :ktable [_ entity]
  (-> (ws/coerce-to-kstream (::kstreams-object entity))
      (streams/map (fn [[k v]]
                     [(if (instance? Windowed k) (.key k) k)
                      v]))))


(defmulti entity->ktable (fn [builder entity]
                           (::entity-type entity)))

(defmethod entity->ktable :topic [builder entity]
  (streams/ktable builder entity))

(defmethod entity->ktable :kstream [_ entity]
  (ws/coerce-to-ktable (::kstreams-object entity) entity))

(defmethod entity->ktable :ktable [_ entity]
  (::kstreams-object entity))


(defmulti ->joinable (fn [builder entity]
                       (::entity-type entity)))

(defmethod ->joinable :topic [builder entity]
  (entity->kstream builder entity))

(defmethod ->joinable :kstream [builder entity]
  (entity->kstream builder entity))

(defmethod ->joinable :ktable [builder entity]
  (entity->ktable builder entity))

(defmethod ->joinable :global-ktable [builder entity]
  (::kstreams-object entity))


(def ->groupable ->joinable)


(defn get-join [joins parents]
  (->> joins
       (filter (fn [[k _]] (= (set k) parents)))
       first))


(defn join-entities [builder [join-order join-config] entities]
  (->> join-order
       (map (comp (partial ->joinable builder) entities))
       (apply ws/join join-config)))


(defmulti build-kstreams-object (fn [entity builder parents entities joins]
                                  (::entity-type entity)))


(defmethod build-kstreams-object :topic [entity builder parents entities _]
  (doseq [p (map entities parents)]
    (streams/to (entity->kstream builder p) entity))
  nil)


(defmethod build-kstreams-object :kstream [entity builder parents entities joins]
  (let [kstream (if (wu/single-elem? parents)
                  (entity->kstream builder (get entities (first parents)))
                  (-> (join-entities builder (get-join joins parents) entities)
                      ws/coerce-to-kstream))]
    (cond-> kstream
            (::xform entity) ((if (:willa.overrides/prevent-repartition entity) ws/transduce-stream-values ws/transduce-stream)
                              (::xform entity)))))


(defmethod build-kstreams-object :ktable [entity builder parents entities joins]
  (let [ktable-or-kstream (if (wu/single-elem? parents)
                            (->groupable builder (get entities (first parents)))
                            (join-entities builder (get-join joins parents) entities))
        store-name (or (::store-name entity)
                       (str (hash parents)))]
    (cond-> ktable-or-kstream
            (::window entity) (ws/coerce-to-kstream)
            (::group-by-fn entity) (streams/group-by (::group-by-fn entity) ws/default-serdes)
            (::window entity) (ws/window-by (::window entity))
            (::aggregate-adder-fn entity) (ws/aggregate (::aggregate-initial-value entity)
                                                        (::aggregate-adder-fn entity)
                                                        (::aggregate-subtractor-fn entity)
                                                        store-name)
            true (ws/coerce-to-ktable entity)
            (::suppression entity) (ws/suppress (::suppression entity)))))


(defmethod build-kstreams-object :global-ktable [entity builder parents entities joins]
  (if (wu/single-elem? parents)
    (let [parent (get entities (first parents))
          parent-type (::entity-type parent)]
      (if (= :topic parent-type)
        (streams/global-ktable builder parent)
        (throw (ex-info (str "GlobalKTable's source must be a :topic and not a " (or parent-type "nil"))
                        {:entity entity :parents parents}))))
    (throw (ex-info "GlobalKTable can only have one source (i.e. no joins)"
                    {:entity entity :parents parents}))))



(defn- build-topology!* [builder {:keys [workflow entities joins]} overrides]
  (let [g (wu/->graph workflow)]
    (->> g
         (lalg/topsort)
         (map (juxt identity (partial l/predecessors g)))
         (reduce (fn [built-entities [node parents]]
                   (let [build-fn (get overrides node build-kstreams-object)]
                     (update built-entities node
                             (fn [entity]
                               (->> (build-fn entity builder parents built-entities joins)
                                    (assoc entity ::kstreams-object))))))
                 entities))))


(def build-topology-unsafe! build-topology!*)


(defn build-topology! [builder topology]
  (build-topology!* builder topology {}))
