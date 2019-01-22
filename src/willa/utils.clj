(ns willa.utils)


(defn transform-value [f]
  (fn [[k v]] [k (f v)]))


(defn transform-values [f]
  (fn [[k v]] (map vector (repeat k) (f v))))


(defn value-pred [f]
  (fn [[k v]] (f v)))


(defn single-elem? [xs]
  (= (count xs) 1))
