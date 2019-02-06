(ns willa.utils
  (:require [loom.graph :as l]))


(defn transform-value [f]
  (fn [[k v]] [k (f v)]))


(defn transform-key [f]
  (fn [[k v]] [(f k) v]))


(defn transform-values [f]
  (fn [[k v]] (map vector (repeat k) (f v))))


(defn value-pred [f]
  (fn [[k v]] (f v)))


(defn key-pred [f]
  (fn [[k v]] (f k)))


(defn single-elem? [xs]
  (= (count xs) 1))


(defn ->graph [workflow]
  (apply l/digraph workflow))


(defn leaves [workflow]
  (let [g (->graph workflow)]
    (->> g
         (l/nodes)
         (filter #(empty? (l/successors g %)))
         set)))


(defn get-topic-name->metadata [entities]
  (->> entities
       (filter (fn [[k v]]
                 (= :topic (:willa.core/entity-type v))))
       (map (fn [[_ t]] [(:topic-name t) t]))
       (into {})))
