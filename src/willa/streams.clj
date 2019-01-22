(ns willa.streams
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn])
  (:import (jackdaw.streams.interop CljKTable CljKStream CljKGroupedTable)
           (org.apache.kafka.streams.kstream Transformer SessionWindows TimeWindows)
           (org.apache.kafka.streams.processor ProcessorContext)))


(def default-serdes
  {:key-serde (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})


(defmulti coerce-to-kstream class)

(defmethod coerce-to-kstream CljKTable [ktable]
  (streams/to-kstream ktable))

(defmethod coerce-to-kstream CljKStream [kstream]
  kstream)


(defmulti coerce-to-ktable class)

(defmethod coerce-to-ktable CljKTable [ktable]
  ktable)

(defmethod coerce-to-ktable CljKStream [kstream]
  (-> kstream
      (streams/group-by-key)
      (streams/reduce (fn [_ x] x) (merge {:topic-name (str (gensym))}
                                          default-serdes))))


(defn aggregate [aggregatable initial-value adder-fn subtractor-fn]
  (let [topic-config (merge {:topic-name (str (gensym))}
                            default-serdes)]
    (if (instance? CljKGroupedTable aggregatable)
      (streams/aggregate
        aggregatable
        (constantly initial-value)
        adder-fn
        subtractor-fn
        topic-config)
      (streams/aggregate
        aggregatable
        (constantly initial-value)
        adder-fn
        topic-config))))


(defmulti join* (fn [join-config kstream-or-ktable other join-fn]
                  [(:type join-config) (class kstream-or-ktable) (class other)]))
(defmethod join*
  [:inner CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/join-windowed kstream other join-fn (:window join-config)
                         default-serdes default-serdes))

(defmethod join*
  [:left CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/left-join-windowed kstream other join-fn (:window join-config)
                              default-serdes default-serdes))

(defmethod join*
  [:outer CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/outer-join-windowed kstream other join-fn (:window join-config)
                               default-serdes default-serdes))

(defmethod join*
  [:inner CljKTable CljKTable]
  [_ ktable other join-fn]
  (streams/join ktable other join-fn))

(defmethod join*
  [:left CljKTable CljKTable]
  [_ ktable other join-fn]
  (streams/left-join ktable other join-fn default-serdes default-serdes))

(defmethod join*
  [:outer CljKTable CljKTable]
  [_ ktable other join-fn]
  (streams/outer-join ktable other join-fn))

(defmethod join*
  [:outer CljKStream CljKTable]
  [_ kstream ktable join-fn]
  (streams/left-join kstream ktable join-fn default-serdes default-serdes))


(defn join
  ([_ joinable] joinable)
  ([join-config joinable other]
   (join* join-config joinable other (fn [v1 v2] [v1 v2])))
  ([join-config joinable o1 o2 & others]
   (apply join
          join-config
          (join* join-config (join join-config joinable o1) o2 (fn [vs v] (conj vs v)))
          others)))


(deftype TransducerTransformer [xform ^{:volatile-mutable true} context]
  Transformer
  (init [_ c]
    (set! context c))
  (transform [_ k v]
    (let [rf (fn
               ([context] context)
               ([^ProcessorContext context [k v]]
                (.forward context k v)
                (.commit context)
                context))]
      ((xform rf) context [k v]))
    nil)
  (close [_]))


(defn transduce-stream [kstream xform]
  (streams/transform kstream #(TransducerTransformer. xform nil)))

(defn window-by [kgroupedstream window]
  (cond
    (instance? SessionWindows window) (streams/window-by-session kgroupedstream window)
    (instance? TimeWindows window) (streams/window-by-time kgroupedstream window)))
