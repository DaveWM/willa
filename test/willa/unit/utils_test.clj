(ns willa.unit.utils-test
  (:require [clojure.test :refer :all]
            [willa.utils :refer :all]
            [loom.graph]))


(deftest transform-value-test
  (is (= [:k 2] ((transform-value inc) [:k 1]))))

(deftest transform-key-test
  (is (= ["ab" 1] ((transform-key #(str % "b")) ["a" 1]))))

(deftest transform-values-test
  (is (= [[:k 0] [:k 2]]
         ((transform-values (juxt dec inc)) [:k 1]))))

(deftest value-pred-test
  (is (true? ((value-pred even?) [:k 2])))
  (is (false? ((value-pred odd?) [:k 2]))))

(deftest key-pred-test
  (is (true? ((key-pred even?) [2 "value"])))
  (is (false? ((key-pred odd?) [2 "value"]))))

(deftest single-elem?-test
  (is (single-elem? [:a]))
  (is (not (single-elem? [])))
  (is (not (single-elem? [:a :b]))))

(deftest ->graph-test
  (is (satisfies? loom.graph/Digraph (->graph [[:a :b]])))
  (is (= (loom.graph/nodes (->graph [[:a :b] [:b :c]]))
         #{:a :b :c})))

(deftest leaves-test
  (is (= #{:c :d} (leaves [[:a :b] [:b :c] [:b :d]]))))

(deftest roots-test
  (is (= #{:a :b} (roots [[:a :c] [:b :c]]))))

(deftest get-topic-name->metadata-test
  (is (= {"input" {:willa.core/entity-type :topic
                   :topic-name "input"}
          "output" {:willa.core/entity-type :topic
                    :topic-name "output"}}
         (get-topic-name->metadata {:input-topic {:willa.core/entity-type :topic
                                                  :topic-name "input"}
                                    :output-topic {:willa.core/entity-type :topic
                                                   :topic-name "output"}
                                    :stream {:willa.core/entity-type :kstream}}))))
