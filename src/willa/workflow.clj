(ns willa.workflow
  (:require [willa.utils :as wu]
            [willa.core :as w]))


(defn dedupe-entities
  ([name] (dedupe-entities name identity))
  ([name id-fn]
   {(keyword "dedupe" (str name "-table")) {::w/entity-type :ktable
                                            ::w/group-by-fn (fn [[k v]] k)
                                            ::w/aggregate-adder-fn (fn [[prev-v known-ids] [k v]]
                                                                     (let [id (id-fn v)]
                                                                       [(when-not (contains? known-ids v) id)
                                                                        (conj known-ids id)]))
                                            ::w/aggregate-initial-value [nil #{}]}
    (keyword "dedupe" (str name "-stream")) {::w/entity-type :kstream
                                             ::w/xform (comp (map (wu/transform-value first))
                                                             (filter (wu/value-pred some?)))}}))


(defn with-dedupe [name from to]
  [[from (keyword "dedupe" (str name "-table"))]
   [(keyword "dedupe" (str name "-table")) (keyword "dedupe" (str name "-stream"))]
   [(keyword "dedupe" (str name "-stream")) to]])
