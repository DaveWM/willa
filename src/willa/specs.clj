(ns willa.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen]
            [willa.utils :as wu]
            [loom.alg :as lalg]
            [loom.graph :as l]
            [jackdaw.serdes.edn])
  (:import (org.apache.kafka.streams.kstream JoinWindows Suppressed Suppressed$BufferConfig)
           (org.apache.kafka.common.serialization Serde)))


(s/def :topic/topic-name string?)
(s/def :topic/serde
  (s/with-gen
    #(instance? Serde %)
    #(gen/return (jackdaw.serdes.edn/serde))))
(s/def :topic/key-serde :topic/serde)
(s/def :topic/value-serde :topic/serde)


(s/def :willa.core/entity-type #{:topic :kstream :ktable})

(s/def :willa.core/join-type #{:merge :left :inner :outer})

(s/def :willa.core/xform
  (s/with-gen fn? #(gen/return (map identity))))

(s/def :willa.core/window
  (s/with-gen #(instance? JoinWindows %)
              (fn []
                (gen/fmap #(JoinWindows/of ^int (Math/abs (unchecked-int %))) (gen/int)))))

(s/def :willa.core/group-by-fn
  (s/with-gen fn?
              #(gen/return identity)))

(s/def :willa.core/aggregate-adder-fn
  (s/with-gen fn?
              #(gen/return (fn [acc kv] kv))))

(s/def :willa.core/aggregate-subtractor-fn
  (s/with-gen fn?
              #(gen/return (fn [acc kv] kv))))

(s/def :willa.core/aggregate-initial-value any?)

(s/def :willa.core/suppression
  (s/with-gen #(instance? Suppressed %)
              #(gen/return (Suppressed/untilWindowCloses (Suppressed$BufferConfig/unbounded)))))


(defmulti entity-spec :willa.core/entity-type)

(defmethod entity-spec :topic [_]
  (s/keys
    :req [:willa.core/entity-type]
    :req-un [:topic/topic-name
             :topic/key-serde
             :topic/value-serde]))

(defmethod entity-spec :kstream [_]
  (s/keys
    :req [:willa.core/entity-type]
    :opt [:willa.core/xform]))

(defmethod entity-spec :ktable [_]
  (s/keys
    :req [:willa.core/entity-type]
    :opt [:willa.core/window
          :willa.core/suppression
          :willa.core/aggregate-subtractor-fn
          :willa.core/group-by-fn
          :willa.core/aggregate-adder-fn
          :willa.core/aggregate-initial-value]))

(s/def ::entity (s/multi-spec entity-spec :willa.core/entity-type))

(s/def ::entity-key
  (s/with-gen keyword?
              #(->> (range (int \a) (inc (int \d)))
                    (map (comp keyword str char))
                    (into #{})
                    (s/gen))))


(defmulti join-config-spec :willa.core/join-type)

(defmethod join-config-spec :merge [_]
  (s/keys :req [:willa.core/join-type]))

(defmethod join-config-spec :default [_]
  (s/keys :req [:willa.core/join-type
                :willa.core/window]))

(s/def ::join
  (s/multi-spec join-config-spec (fn [gen-v dispatch-tag] gen-v)))

(s/def ::join-keys (s/coll-of ::entity-key))


(s/def ::workflow (s/and (s/coll-of (s/and (s/tuple ::entity-key ::entity-key)
                                           (fn [[x y]] (not= x y)))
                                    :min-count 1
                                    :distinct true)
                         (fn dag? [workflow]
                           (lalg/dag? (wu/->graph workflow)))))

(s/def ::entities (s/map-of ::entity-key ::entity))

(s/def ::joins (s/map-of ::join-keys ::join))


(s/def ::topology
  (let [all-roots-topics?  (fn [{:keys [workflow entities]}]
                             (->> (wu/roots workflow)
                                  (map entities)
                                  (every? #(= :topic (:willa.core/entity-type %)))))
        all-leaves-topics? (fn [{:keys [workflow entities]}]
                             (->> (wu/leaves workflow)
                                  (map entities)
                                  (every? #(= :topic (:willa.core/entity-type %)))))]
    (s/with-gen
      (s/and (s/keys :req-un [::workflow
                              ::entities]
                     :opt-un [::joins])
             (fn all-entities-present? [{:keys [workflow entities]}]
               (let [all-workflow-entities (->> workflow
                                                flatten
                                                set)
                     all-entities          (->> entities
                                                (map key)
                                                set)]
                 (clojure.set/subset? all-workflow-entities all-entities)))
             all-roots-topics?
             all-leaves-topics?)
      (fn []
        (let [workflow-gen           (s/gen ::workflow)
              workflow->entities-gen (fn [workflow]
                                       (let [workflow-keys (->> workflow
                                                                flatten
                                                                set)]
                                         (s/gen ::entities {::entity-key #(s/gen workflow-keys)})))
              workflow->joins-gen    (fn [workflow]
                                       (let [workflow-graph  (wu/->graph workflow)
                                             joined-entities (->> workflow-graph
                                                                  l/nodes
                                                                  (map #(l/predecessors workflow-graph %))
                                                                  (map vec)
                                                                  (filter #(< 1 (count %)))
                                                                  set)]
                                         (if (not-empty joined-entities)
                                           (s/gen ::joins {::join-keys #(s/gen joined-entities)})
                                           (gen/return {}))))]
          (as-> workflow-gen $
                (gen/bind $
                          (fn [workflow]
                            (gen/hash-map :workflow (gen/return workflow)
                                          :entities (workflow->entities-gen workflow)
                                          :joins (workflow->joins-gen workflow))))
                (gen/such-that (every-pred all-roots-topics? all-leaves-topics?) $)))))))
