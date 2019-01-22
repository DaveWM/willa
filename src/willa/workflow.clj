(ns willa.workflow
  (:require [willa.utils :as wu]))


(defn dedupe-entities [name]
  {(keyword "dedupe" (str name "-table")) {:type :ktable
                                           :group-by (fn [[k v]] k)
                                           :aggregate-adder (fn [[prev-v known-vs] [k v]]
                                                              [(when-not (contains? known-vs v) v)
                                                               (conj known-vs v)])
                                           :initial-value [nil #{}]}
   (keyword "dedupe" (str name "-stream")) {:type :kstream
                                            :xform (comp (map (wu/transform-value first))
                                                         (filter (wu/value-pred some?)))}})


(defn with-dedupe [name from to]
  [[from (keyword "dedupe" (str name "-table"))]
   [(keyword "dedupe" (str name "-table")) (keyword "dedupe" (str name "-stream"))]
   [(keyword "dedupe" (str name "-stream")) to]])
