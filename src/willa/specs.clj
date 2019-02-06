(ns willa.specs
  (:require [clojure.spec.alpha :as s]
            [clojure.spec.gen.alpha :as gen])
  (:import (org.apache.kafka.streams.kstream JoinWindows)))


(s/def ::entity-key
  (s/with-gen keyword?
              #(->> (range (int \a) (inc (int \z)))
                    (map (comp keyword str char))
                    (into #{})
                    (s/gen))))

(s/def ::workflow (s/coll-of (s/tuple ::entity-key ::entity-key)))


(s/def ::entities (s/map-of ::entity-key ::entity))

(s/def :willa.core/xform
  (s/with-gen fn? #(gen/return (map identity))))

(s/def :topic/topic-name string?)

(s/def :willa.core/entity-type #{:topic :kstream :ktable})

(defmulti entity-spec :willa.core/entity-type)

(defmethod entity-spec :topic [_]
  (s/keys
    :req [:willa.core/entity-type]
    :req-un [:topic/topic-name]))

(defmethod entity-spec :kstream [_]
  (s/keys
    :req [:willa.core/entity-type]
    :opt [:willa.core/xform]))

(s/def ::entity (s/multi-spec entity-spec :willa.core/entity-type))


(s/def ::joins (s/map-of (s/coll-of ::entity-key) ::join))

(s/def :willa.core/window
  (s/with-gen #(instance? JoinWindows %)
              (fn []
                (gen/fmap #(JoinWindows/of ^int (Math/abs (unchecked-int %))) (gen/int)))))

(s/def :willa.core/join-type #{:merge :left :inner :outer})

(defmulti join-config-spec :willa.core/join-type)

(defmethod join-config-spec :merge [_]
  (s/keys :req [:willa.core/join-type]))

(defmethod join-config-spec :default [_]
  (s/keys :req [:willa.core/join-type
                :willa.core/window]))

(s/def ::join
  (s/multi-spec join-config-spec (fn [gen-v dispatch-tag] gen-v)))


(s/def ::world
  (s/and (s/keys :req-un [::workflow
                          ::entities]
                 :opt-un [::joins])
         (fn [{:keys [workflow entities]}]
           (let [all-workflow-entities (->> workflow
                                            flatten
                                            set)
                 all-entities (->> entities
                                   (map key)
                                   set)]
             (clojure.set/subset? all-workflow-entities all-entities)))))
