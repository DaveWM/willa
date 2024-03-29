(ns willa.streams
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn]
            [jackdaw.streams.lambdas]
            [jackdaw.streams.interop]
            [clojure.set :as set])
  (:import (jackdaw.streams.interop CljKTable CljKStream CljKGroupedTable CljGlobalKTable)
           (org.apache.kafka.streams.kstream Transformer SessionWindows TimeWindows KTable ValueTransformerWithKey KStream ValueTransformerWithKeySupplier)
           (org.apache.kafka.streams.processor ProcessorContext)))


(def default-serdes
  {:key-serde   (serdes.edn/serde)
   :value-serde (serdes.edn/serde)})


(defmulti coerce-to-kstream class)

(defmethod coerce-to-kstream CljKTable [ktable]
  (streams/to-kstream ktable))

(defmethod coerce-to-kstream CljKStream [kstream]
  kstream)


(defmulti coerce-to-ktable (fn [kstreams-object entity] (class kstreams-object)))

(defmethod coerce-to-ktable CljKTable [ktable {:keys [:willa.core/store-name] :as config}]
  (if (or (nil? store-name)
          (= store-name (.queryableStoreName (streams/ktable* ktable))))
    ktable
    (coerce-to-ktable (streams/to-kstream ktable) config)))

(defmethod coerce-to-ktable CljKStream [kstream {:keys [:willa.core/store-name] :as config}]
  (-> kstream
      (streams/group-by-key default-serdes)
      (streams/reduce (fn [_ x] x)
                      (-> default-serdes
                          (assoc :topic-name (or store-name (str (gensym))))))))

(defn aggregate [aggregatable initial-value adder-fn subtractor-fn store-name]
  (let [topic-config (merge {:topic-name store-name}
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
                  [(:willa.core/join-type join-config) (class kstream-or-ktable) (class other)]))
(defmethod join*
  [:inner CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/join-windowed kstream other join-fn (:willa.core/window join-config)
                         default-serdes default-serdes))

(defmethod join*
  [:left CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/left-join-windowed kstream other join-fn (:willa.core/window join-config)
                              default-serdes default-serdes))

(defmethod join*
  [:outer CljKStream CljKStream]
  [join-config kstream other join-fn]
  (streams/outer-join-windowed kstream other join-fn (:willa.core/window join-config)
                               default-serdes default-serdes))


(defmethod join*
  [:merge CljKStream CljKStream]
  [_ kstream other _]
  (streams/merge kstream other))

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
  [:left CljKStream CljKTable]
  [_ kstream ktable join-fn]
  (streams/left-join kstream ktable join-fn default-serdes default-serdes))

(defmethod join*
  [:inner CljKStream CljGlobalKTable]
  [{kv-mapper :willa.core/kv-mapper, :or {kv-mapper first}} kstream global-ktable join-fn]
  (streams/join-global kstream global-ktable kv-mapper join-fn))

(defmethod join*
  [:left CljKStream CljGlobalKTable]
  [{kv-mapper :willa.core/kv-mapper, :or {kv-mapper first}} kstream global-ktable join-fn]
  (streams/left-join-global kstream global-ktable kv-mapper join-fn))


(defn join
  ([_ joinable] joinable)
  ([join-config joinable other]
   (join* join-config joinable other (fn [v1 v2] [v1 v2])))
  ([join-config joinable o1 o2 & others]
   (apply join
          join-config
          (join* join-config (join join-config joinable o1) o2 (fn [vs v] (conj vs v)))
          others)))


(deftype TransducerTransformer [xform]
  Transformer
  (init [_ _context])
  (transform [_ k v]
    (->> ((xform conj) [] [k v])
         (map jackdaw.streams.lambdas/key-value)))
  (close [_]))


(deftype ValueTransducerTransformer [xform]
  ValueTransformerWithKey
  (init [_ _context])
  (transform [_ k v]
    (->> ((xform conj) [] [k v])
         (map second)))
  (close [_]))


(defn transduce-stream [kstream xform]
  (streams/flat-transform kstream #(TransducerTransformer. xform)))


(defn- flat-transform-values
  [kstream value-transformer-supplier-fn]
  (jackdaw.streams.interop/clj-kstream
    (.flatTransformValues ^KStream (jackdaw.streams/kstream* kstream)
                          ^ValueTransformerWithKeySupplier (reify
                                                             ValueTransformerWithKeySupplier
                                                             (get [_this]
                                                               (value-transformer-supplier-fn)))
                          ^"[Ljava.lang.String;" (into-array String []))))


(defn transduce-stream-values [kstream xform]
  (flat-transform-values kstream #(ValueTransducerTransformer. xform)))

(defn window-by [kgroupedstream window]
  (cond
    (instance? SessionWindows window) (streams/window-by-session kgroupedstream window)
    (instance? TimeWindows window) (streams/window-by-time kgroupedstream window)))


;; TODO replace with Jackdaw implementation once https://github.com/FundingCircle/jackdaw/pull/23 has been merged & released
(defn suppress [ktable suppression]
  (jackdaw.streams.interop/clj-ktable
    (^KTable .suppress (jackdaw.streams.protocols/ktable* ktable)
                       suppression)))
