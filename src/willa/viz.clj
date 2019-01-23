(ns willa.viz
  (:require [loom.graph :as l]
            [rhizome.viz :as r]
            [rhizome.dot :as rd]
            [willa.core :as w]))


(def entity-type->shape
  {:topic "cylinder"
   :kstream "rectangle"
   :ktable "component"})


(def entity-type->colour
  {:topic "palegoldenrod"
   :kstream "palegreen2"
   :ktable "paleturquoise"})


(defn- make-image
  ([workflow entities] (make-image workflow entities {}))
  ([workflow entities {:keys [descriptor-fn] :or {descriptor-fn (constantly {})}}]
   (let [g               (apply l/digraph workflow)
         nodes           (l/nodes g)
         nodes->adjacent (->> nodes
                              (map (juxt identity (partial l/successors g)))
                              (remove (fn [[n successors]] (nil? successors)))
                              (into {}))]
     (-> (rd/graph->dot nodes nodes->adjacent
                       :node->descriptor (fn [n]
                                           (let [{:keys [willa.core/entity-type] :as entity} (get entities n)]
                                             (merge
                                               {:label n
                                                :color "black"
                                                :fillcolor (entity-type->colour entity-type)
                                                :style "filled"
                                                :penwidth 1.3
                                                :height 0.7
                                                :width 0.9
                                                :shape (entity-type->shape entity-type)}
                                               (descriptor-fn n entity)))))
         (r/dot->image)))))


(def view-workflow (comp r/view-image make-image))


(defn save-workflow [filename & args]
  (-> (apply make-image args)
      (r/save-image filename)))


(comment

  (view-workflow [[:topics/input-topic :stream]
                  [:stream :table]
                  [:table :topics/output-topic]]
                 {:topics/input-topic {::w/entity-type :topic}
                  :stream {::w/entity-type :kstream}
                  :table {::w/entity-type :ktable}
                  :topics/output-topic {::w/entity-type :topic}})
  )
