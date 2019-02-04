(ns willa.experiment-core-congruity-test
  (:require [clojure.test :refer :all]
            [willa.core :as w]
            [willa.experiment :as we]
            [jackdaw.streams :as streams]
            [willa.streams :as ws]
            [jackdaw.test :as jt]
            [willa.utils :as wu]
            [loom.graph :as l]
            [loom.alg :as lalg])
  (:import (java.util Properties)
           (org.apache.kafka.streams TopologyTestDriver)
           (org.apache.kafka.streams.kstream JoinWindows TimeWindows Suppressed Suppressed$BufferConfig)))


(defn test-driver
  [builder app-config]
  (let [topology (-> (streams/streams-builder* builder)
                     (.build))]
    (TopologyTestDriver. topology
                         (let [props (Properties.)]
                           (doseq [[k v] app-config]
                             (.setProperty props k v))
                           props)
                         0)))


(defn ->topic [name]
  (merge {:topic-name name
          :replication-factor 1
          :partition-count 1
          ::w/entity-type :topic}
         ws/default-serdes))


(defn equal-on [ks m1 m2]
  (= (select-keys m1 ks)
     (select-keys m2 ks)))


(defn leaves [workflow]
  (let [g (apply l/digraph workflow)]
    (->> g
         (l/nodes)
         (filter #(empty? (l/successors g %)))
         set)))


(defmulti transport (fn [type builder config world]
                      type))

(defmethod transport :mock [_ builder config world]
  (let [driver (test-driver builder config)]
    (-> (jt/mock-transport {:driver driver}
                           (->> (:entities world)
                                (filter (fn [[k# v#]]
                                          (= :topic (::w/entity-type v#))))
                                (map (fn [[_# t#]] [(:topic-name t#) t#]))
                                (into {})))
        (update :exit-hooks conj (fn [] (.close driver))))))


(defmacro with-test-machine [name {:keys [transport-type builder world]} & body]
  `(let [config# {"application.id" "test"
                  "bootstrap.servers" "localhost:9092"
                  "cache.max.bytes.buffering" "0"}]
     (with-open [~name (jt/test-machine (transport ~transport-type ~builder config# ~world))]
       ~@body)))


(defn exercise-workflow [transport-type world inputs]
  (let [experiment-entities (:entities (we/run-experiment world inputs))
        builder             (doto (streams/streams-builder)
                              (w/build-workflow! world))
        output-topic-keys   (leaves (:workflow world))]
    (with-test-machine tm {:builder builder
                           :world world
                           :transport-type transport-type}
      (let [results               (jt/run-test
                                    tm
                                    (concat
                                      (->> inputs
                                           (mapcat (fn [[e rs]]
                                                     (map vector (repeat e) rs)))
                                           (sort-by (wu/value-pred :timestamp))
                                           (map (fn [[e r]]
                                                  [:write!
                                                   (:topic-name (get experiment-entities e))
                                                   (:value r) {:key (:key r)
                                                               :timestamp (:timestamp r)}])))
                                      [[:watch (fn [journal]
                                                 (->> output-topic-keys
                                                      (every? (fn [t]
                                                                (= (count (get-in journal [:topics (:topic-name (get experiment-entities t))]))
                                                                   (count (get-in experiment-entities [t ::we/output])))))))]]))
            test-topology-results (->> (get-in results [:journal :topics])
                                       (map (wu/transform-key #(->> (:entities world)
                                                                    (filter (fn [[k v]] (= % (:topic-name v))))
                                                                    (map key)
                                                                    first)))
                                       (map (wu/transform-value (partial sort-by :offset)))
                                       (filter (wu/key-pred output-topic-keys))
                                       (into {}))
            experiment-results    (->> experiment-entities
                                       (filter (wu/key-pred output-topic-keys))
                                       (map (wu/transform-value #(->> (::we/output %)
                                                                      (sort-by :timestamp))))
                                       (into {}))]
        {:official-results test-topology-results
         :experiment-results experiment-results}))))


(defn congruous-with-mock? [world inputs]
  (let [{:keys [official-results experiment-results]} (exercise-workflow :mock world inputs)
        output-topics (leaves (:workflow world))
        ->comparable (fn [m]
                       (->> (select-keys m output-topics)
                            (map (wu/transform-value (partial map #(select-keys % [:key :value]))))))]
    (= (->comparable official-results)
       (->comparable experiment-results))))


(deftest basic-topology-tests

  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :output-topic (->topic "output-topic")
                                        :stream {::w/entity-type :kstream}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 0}]}))

  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :output-topic (->topic "output-topic")
                                        :stream {::w/entity-type :kstream
                                                 ::w/xform (map (wu/transform-value inc))}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 0}]}))

  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:stream :secondary-stream]
                                        [:secondary-stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :output-topic (->topic "output-topic")
                                        :stream {::w/entity-type :kstream
                                                 ::w/xform (map (wu/transform-value inc))}
                                        :secondary-stream {::w/entity-type :kstream
                                                           ::w/xform (filter (wu/value-pred even?))}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 0}
                                           {:key "k" :value 2 :timestamp 50}]}))

  (is (congruous-with-mock? {:workflow [[:input-topic :table]
                                        [:table :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :output-topic (->topic "output-topic")
                                        :table {::w/entity-type :ktable}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 0}
                                           {:key "k" :value 2 :timestamp 50}]})))


(deftest aggregation-tests

  (is (congruous-with-mock? {:workflow [[:input-topic :table]
                                        [:table :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :output-topic (->topic "output-topic")
                                        :table {::w/entity-type :ktable
                                                ::w/window (TimeWindows/of 100)
                                                ::w/group-by-fn (fn [[k v]] k)
                                                ::w/aggregate-initial-value 0
                                                ::w/aggregate-adder-fn (fn [acc [k v]]
                                                                         (+ acc v))}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 0}
                                           {:key "k" :value 2 :timestamp 50}]}))

  (is (congruous-with-mock? {:workflow [[:input-topic :table]
                                        [:table :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :output-topic (->topic "output-topic")
                                        :table {::w/entity-type :ktable
                                                ::w/window (TimeWindows/of 100)
                                                ::w/group-by-fn (fn [[k v]] k)
                                                ::w/aggregate-initial-value 0
                                                ::w/aggregate-adder-fn (fn [acc [k v]]
                                                                         (+ acc v))}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 0}
                                           {:key "k" :value 2 :timestamp 500}]})))


(deftest joins-tests

  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:secondary-input-topic :stream]
                                        [:stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :secondary-input-topic (->topic "secondary-input-topic")
                                        :stream {::w/entity-type :kstream
                                                 ::w/xform (map (fn [[k v]]
                                                                  [k (apply + (remove nil? v))]))}
                                        :output-topic (->topic "output-topic")}
                             :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                            ::w/window (JoinWindows/of 100)}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 100}]
                             :secondary-input-topic [{:key "k" :value 2 :timestamp 150}]}))


  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:secondary-input-topic :stream]
                                        [:stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :secondary-input-topic (->topic "secondary-input-topic")
                                        :stream {::w/entity-type :kstream
                                                 ::w/xform (map (fn [[k v]]
                                                                  [k (apply + (remove nil? v))]))}
                                        :output-topic (->topic "output-topic")}
                             :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                            ::w/window (JoinWindows/of 100)}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 150}]
                             :secondary-input-topic [{:key "k" :value 2 :timestamp 100}]}))

  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:secondary-input-topic :stream]
                                        [:stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :secondary-input-topic (->topic "secondary-input-topic")
                                        :stream {::w/entity-type :kstream
                                                 ::w/xform (map (fn [[k v]]
                                                                  [k (apply + (remove nil? v))]))}
                                        :output-topic (->topic "output-topic")}
                             :joins {[:input-topic :secondary-input-topic] {::w/join-type :inner
                                                                            ::w/window (JoinWindows/of 100)}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 100}]
                             :secondary-input-topic [{:key "k" :value 2 :timestamp 150}]}))

  (is (congruous-with-mock? {:workflow [[:input-topic :stream]
                                        [:secondary-input-topic :stream]
                                        [:stream :output-topic]]
                             :entities {:input-topic (->topic "input-topic")
                                        :secondary-input-topic (->topic "secondary-input-topic")
                                        :stream {::w/entity-type :kstream
                                                 ::w/xform (map (fn [[k v]]
                                                                  [k (apply + (remove nil? v))]))}
                                        :output-topic (->topic "output-topic")}
                             :joins {[:input-topic :secondary-input-topic] {::w/join-type :left
                                                                            ::w/window (JoinWindows/of 10)}}}
                            {:input-topic [{:key "k" :value 1 :timestamp 150}]
                             :secondary-input-topic [{:key "k" :value 2 :timestamp 100}]})))
