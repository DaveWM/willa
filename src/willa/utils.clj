(ns willa.utils)


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
