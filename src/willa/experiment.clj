(ns willa.experiment
  (:require [loom.graph :as l]
            [loom.alg :as lalg]
            [willa.core :as w]
            [clojure.math.combinatorics :as combo])
  (:import (org.apache.kafka.streams.kstream JoinWindows)))


(defn join-kstream-results [left-results right-results ^JoinWindows window {:keys [left-join right-join join-fn]
                                                                            :or {join-fn vector}}]
  (let [before-ms              (.beforeMs window)
        after-ms               (.afterMs window)
        joined-results         (->> (combo/cartesian-product left-results right-results)
                                    (filter (fn [[r1 r2]]
                                              (and (= (:key r1) (:key r2))
                                                   (<= (- (:timestamp r1) before-ms) (:timestamp r2))
                                                   (<= (:timestamp r2) (+ (:timestamp r1) after-ms)))))
                                    (map (fn [[l r]]
                                           {:key (:key l)
                                            :timestamp (max (:timestamp l) (or (:timestamp r) 0))
                                            :value (join-fn (:value l) (:value r))})))
        left-unjoined-results  (->> left-results
                                    (map (fn [result] (update result :value cons [nil]))))
        right-unjoined-results (->> right-results
                                    (map (fn [result] (update result :value #(-> [nil %])))))]
    (cond->> joined-results
             left-join (concat left-unjoined-results)
             right-join (concat right-unjoined-results)
             true (group-by :timestamp)
             true (mapcat (fn [[t rs]]
                            (if (< 1 (count rs))
                              (remove (fn [result] (some nil? (:value result))) rs)
                              rs))))))


(defn join-ktable-results [left-results right-results {:keys [left-join right-join join-fn]
                                                       :or {join-fn vector}}]
  (let [sorted-left-results  (sort-by :timestamp left-results)
        sorted-right-results (sort-by :timestamp right-results)
        left-joined          (->> left-results
                                  (map (fn [result]
                                         [result
                                          (->> sorted-right-results
                                               (filter #(and (<= (:timestamp %) (:timestamp result))
                                                             (= (:key %) (:key result))))
                                               last)])))
        right-joined         (->> right-results
                                  (map (fn [result]
                                         [(->> sorted-left-results
                                               (filter #(and (<= (:timestamp %) (:timestamp result))
                                                             (= (:key %) (:key result))))
                                               last)
                                          result])))]
    (->> (concat left-joined right-joined)
         (filter (fn [[l r]]
                   (and (or (not left-join) (some? l))
                        (or (not right-join) (some? r)))))
         (map (fn [[l r]]
                {:key (:key (or l r))
                 :value (join-fn (:value l) (:value r))
                 :timestamp ((fnil max 0 0) (:timestamp l) (:timestamp r))})))))


(defmulti join-entities* (fn [join-config entity other join-fn]
                           [(::w/join-type join-config) (::w/entity-type entity) (::w/entity-type other)]))

(defmethod join-entities* [:inner :kstream :kstream] [join-config entity other join-fn]
  (assoc entity ::results
                (join-kstream-results (::results entity) (::results other) (::w/window join-config) {:left-join false
                                                                                                     :right-join false
                                                                                                     :join-fn join-fn})))

(defmethod join-entities* [:left :kstream :kstream] [join-config entity other join-fn]
  (assoc entity ::results
                (join-kstream-results (::results entity) (::results other) (::w/window join-config) {:left-join true
                                                                                                     :right-join false
                                                                                                     :join-fn join-fn})))

(defmethod join-entities* [:outer :kstream :kstream] [join-config entity other join-fn]
  (assoc entity ::results
                (join-kstream-results (::results entity) (::results other) (::w/window join-config) {:left-join true
                                                                                                     :right-join true
                                                                                                     :join-fn join-fn})))

(defmethod join-entities* [:merge :kstream :kstream] [_ entity other _]
  (update entity ::results concat (::results other)))

(defmethod join-entities* [:inner :ktable :ktable] [_ entity other join-fn]
  (assoc entity ::results (join-ktable-results (::results entity) (::results other) {:left-join true
                                                                                     :right-join true
                                                                                     :join-fn join-fn})))

(defmethod join-entities* [:left :ktable :ktable] [_ entity other join-fn]
  (assoc entity ::results (join-ktable-results (::results entity) (::results other) {:left-join true
                                                                                     :right-join false
                                                                                     :join-fn join-fn})))

(defmethod join-entities* [:outer :ktable :ktable] [_ entity other join-fn]
  (assoc entity ::results (join-ktable-results (::results entity) (::results other) {:left-join false
                                                                                     :right-join false
                                                                                     :join-fn join-fn})))


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
