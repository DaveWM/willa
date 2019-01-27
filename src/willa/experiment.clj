(ns willa.experiment
  (:require [loom.graph :as l]
            [loom.alg :as lalg]
            [willa.core :as w]
            [clojure.math.combinatorics :as combo])
  (:import (org.apache.kafka.streams.kstream JoinWindows)))


(defmulti join-entities* (fn [join-config entity other join-fn]
                           [(::w/join-type join-config) (::w/entity-type entity) (::w/entity-type other)]))

(defmethod join-entities* [:inner :kstream :kstream] [join-config entity other join-fn]
  (let [before-ms (.beforeMs ^JoinWindows (::w/window join-config))
        after-ms  (.afterMs ^JoinWindows (::w/window join-config))]
    (assoc entity ::results
                  (->> (combo/cartesian-product (::results entity) (::results other))
                       (filter (fn [[r1 r2]]
                                 (and (= (:key r1) (:key r2))
                                      (<= (- (:timestamp r1) before-ms) (:timestamp r2))
                                      (<= (:timestamp r2) (+ (:timestamp r1) after-ms)))))
                       (map (fn [[r1 r2]]
                              {:key (:key r1)
                               :timestamp (max (:timestamp r1) (:timestamp r2))
                               :value (join-fn (:value r1) (:value r2))}))))))

(defmethod join-entities* [:left :kstream :kstream] [join-config entity other join-fn]
  (let [before-ms        (.beforeMs ^JoinWindows (::w/window join-config))
        after-ms         (.afterMs ^JoinWindows (::w/window join-config))
        joined-results   (->> (combo/cartesian-product (::results entity) (::results other))
                              (filter (fn [[r1 r2]]
                                        (and (= (:key r1) (:key r2))
                                             (<= (- (:timestamp r1) before-ms) (:timestamp r2))
                                             (<= (:timestamp r2) (+ (:timestamp r1) after-ms)))))
                              (map (fn [[l r]]
                                     {:key (:key l)
                                      :timestamp (max (:timestamp l) (or (:timestamp r) 0))
                                      :value (join-fn (:value l) (:value r))})))
        unjoined-results (->> (::results entity)
                              (map (fn [result] (update result :value cons [nil]))))]
    (assoc entity ::results
                  (->> joined-results
                       (concat unjoined-results)
                       (group-by :timestamp)
                       (mapcat (fn [[t rs]]
                                 (if (< 1 (count rs))
                                   (remove (fn [result] (nil? (second (:value result)))) rs)
                                   rs)))))))

(defmethod join-entities* [:outer :kstream :kstream] [join-config entity other join-fn]
  (let [before-ms              (.beforeMs ^JoinWindows (::w/window join-config))
        after-ms               (.afterMs ^JoinWindows (::w/window join-config))
        joined-results         (->> (combo/cartesian-product (::results entity) (::results other))
                                    (filter (fn [[r1 r2]]
                                              (and (= (:key r1) (:key r2))
                                                   (<= (- (:timestamp r1) before-ms) (:timestamp r2))
                                                   (<= (:timestamp r2) (+ (:timestamp r1) after-ms)))))
                                    (map (fn [[l r]]
                                           {:key (:key l)
                                            :timestamp (max (:timestamp l) (or (:timestamp r) 0))
                                            :value (join-fn (:value l) (:value r))})))
        left-unjoined-results  (->> (::results entity)
                                    (map (fn [result] (update result :value cons [nil]))))
        right-unjoined-results (->> (::results other)
                                    (map (fn [result] (update result :value #(-> [nil %])))))]
    (assoc entity ::results
                  (->> joined-results
                       (concat left-unjoined-results)
                       (concat right-unjoined-results)
                       (group-by :timestamp)
                       (mapcat (fn [[t rs]]
                                 (if (< 1 (count rs))
                                   (remove (fn [result] (some nil? (:value result))) rs)
                                   rs)))))))

(defmethod join-entities* [:merge :kstream :kstream] [_ entity other _]
  (update entity ::results concat (::results other)))


(defn ->joinable [entity]
  (if (= :topic (::w/entity-type entity))
    ;; treat topics as streams
    (assoc entity ::w/entity-type :kstream)
    entity))


(defn join-entities
  ([_ entity] entity)
  ([join-config entity other]
   (join-entities* join-config entity other vector))
  ([join-config entity o1 o2 & others]
   (apply join-entities
          join-config
          (join-entities* join-config (join-entities join-config entity o1) o2 (fn [vs v] (conj vs v)))
          others)))


(defmulti process-entity (fn [entity parents entities joins]
                           (::w/entity-type entity)))

(defmethod process-entity :topic [entity parents entities _]
  (if parents
    (assoc entity ::results (->> (map entities parents)
                                 (mapcat ::results)))
    entity))

(defmethod process-entity :kstream [entity parents entities joins]
  (let [[join-order join-config] (w/get-join joins parents)
        joined-entity (if join-order
                        (apply join-entities join-config (map (comp ->joinable entities) join-order))
                        (get entities (first parents)))
        xform         (get entity ::w/xform (map identity))
        results       (->> (for [r (::results joined-entity)]
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

  (require 'willa.viz)


  (def workflow [[:input-topic :stream]
                 [:secondary-input-topic :stream]
                 [:tertiary-input-topic :stream]
                 [:stream :output-topic]])

  (def processed-entities
    (run-experiment {:workflow workflow
                     :entities {:input-topic {::w/entity-type :topic}
                                :secondary-input-topic {::w/entity-type :topic}
                                :tertiary-input-topic {::w/entity-type :topic}
                                :stream {::w/entity-type :kstream
                                         ::w/xform (map (fn [[k vs]]
                                                          [k (apply max (remove nil? vs))]))}
                                :output-topic {::w/entity-type :topic}}
                     :joins {[:input-topic :secondary-input-topic :tertiary-input-topic] {::w/join-type :outer
                                                                                          ::w/window (JoinWindows/of 1000)}}}
                    {:input-topic [{:key "key"
                                    :value 1
                                    :timestamp 500}]
                     :secondary-input-topic [{:key "key"
                                              :value 2
                                              :timestamp 1000}]
                     }))

  (willa.viz/view-workflow {:workflow workflow
                            :entities processed-entities})
  )
