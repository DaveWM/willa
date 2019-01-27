(ns willa.experiment
  (:require [loom.graph :as l]
            [loom.alg :as lalg]
            [willa.core :as w]))


(defmulti process-entity (fn [entity parents entities joins]
                           (::w/entity-type entity)))

(defmethod process-entity :topic [entity parents entities _]
  (if parents
    (assoc entity ::results (->> (map entities parents)
                                 (mapcat ::results)))
    entity))

(defmethod process-entity :kstream [entity parents entities joins]
  (let [[join-order join-config] (w/get-join joins parents)
        parent  (get entities (first parents))
        xform   (get entity ::w/xform (map identity))
        results (->> (for [r (::results parent)]
                       (into []
                             (comp (map (juxt :key :value))
                                   xform
                                   (map (fn [[k v]] (merge r {:key k :value v}))))
                             [r]))
                     (mapcat identity))]
    (assoc entity ::results results)))


(defn run-experiment [{:keys [workflow entities joins]} entity->records]
  (let [g                (apply l/digraph workflow)
        initial-entities (->> entities
                              (map (fn [[k v]] [k (assoc v ::results (get entity->records k))]))
                              (into {}))]
    (->> g
         (lalg/topsort)
         (map (juxt identity (partial l/predecessors g)))
         (reduce (fn [processed-entities [node parents]]
                   (update processed-entities node process-entity parents processed-entities joins))
                 initial-entities))))


(comment

  (def processed-entities
    (run-experiment {:workflow [[:input-topic :stream]
                                [:stream :output-topic]]
                     :entities {:input-topic {::w/entity-type :topic}
                                :stream {::w/entity-type :kstream
                                         ::w/xform (mapcat (fn [[k v]] (map vector (repeat k) ((juxt inc dec) v))))}
                                :output-topic {::w/entity-type :topic}}}
                    {:input-topic [{:key "key"
                                    :value 1
                                    :timestamp 1234}]}))

  (willa.viz/view-workflow {:workflow [[:input-topic :stream]
                                       [:stream :output-topic]]
                            :entities processed-entities})
  )
