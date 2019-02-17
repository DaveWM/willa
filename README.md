# willa [![CircleCI](https://circleci.com/gh/DaveWM/willa.svg?style=svg)](https://circleci.com/gh/DaveWM/willa)
**Alpha**


Willa provides a data-driven DSL on top of the [Kafka Streams DSL](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html),
built on top of Jackdaw.

It is named after [Willa Muir](https://en.wikipedia.org/wiki/Willa_Muir), who translated Kafka's "The Metamorphosis".
Her husband Edwin was also involved, but apparently he "only helped".

## Rationale

The Kafka Streams DSL is very "Javaish". 
It uses a `KStreamsBuilder` object to build topologies, which operates by in-place mutation.
This has all the [usual disadvantages](https://clojure.org/about/state#_object_oriented_programming_oo) of mutability, 
including making topologies more difficult to compose, test, and visualise.
You do get a `ProcessorTopology` object from the `build` method, which you can theoretically use to manipulate the built topology. However, this is a mutable API, and is extremely difficult to work with in practice - not least because the `ProcessorTopology` class isn't documented.
The `KStreamsBuilder` API also re-implements many of the stateless transformation functions from the core Clojure library (`map`, `filter`, `mapcat`, etc.), encouraging needless code duplication.

Willa aims to provide an immutable, data-driven DSL (inspired by Onyx) on top of the Kafka Streams DSL.
It represents all aspects of your topology as Clojure data structures and functions.
This makes topologies far easier to manipulate and compose. For example, if you want to log every message that is published to output topics,
you can write a generic pure function to transform a Willa topology to achieve this.
It also enables you to visualise your topology using GraphViz, which is very useful for reasoning about how a topology works, and also for documentation purposes.

Willa uses transducers for stateless transformations, as opposed to a separate API like with the `KStreamsBuilder`.
Transducers are far more composable, and allow you to re-use code far more effectively.
You can also test your transformation logic completely independently of Kafka.

Willa also provides a mechanism for experimenting with a topology from the repl, and seeing how data flows through it.
It can also be used for unit testing. This mechanism is similar in scope to Kafka's `TestTopologyDriver`, but has a few advantages:
1. It gives you the output data of each individual `KStream`/`KTable`/topic within your topology, instead of just the data on the output topics.
2. It enables you to visualise the data flow using GraphViz.
3. It is faster, and doesn't persist anything on disk.


## Getting Started

Willa represents your topology as a `world`. The `world` is a map, containing 3 keys:
* `:entities` - an entity is a map containing the information needed to build a topic/`kstream`/`KTable`. The `:entities` map is a map of entity name to config.
* `:workflow` - a vector of tuples of `[input-entity-name output-entity-name]`, similar to a `workflow` in Onyx.
* `:joins` - this is a map representing all the joins/merges in your topology as data. It is a map of a vector of entity names to join, to a join config.

This may sound very confusing, but let's try to clear things up with a simple example.
Before we start, make sure you have a Kafka broker running locally, either using the Confluent distribution or Landoop's fast-data-dev docker image.

Say we want a topology that simply reads messages from an input topic, increments the value, then writes to an output topic.
The topology would look like this:

;; TODO - Diagram

Let's start by requiring some necessary namespaces:

```clojure
(ns my-cool-app.core
  (:require [jackdaw.streams :as streams]
            [jackdaw.serdes.edn :as serdes.edn]
            [willa.core :as w]))
```

We then create the workflow like so:

```clojure
(def workflow
  [[:input-topic :increment-stream]
   [:increment-stream :output-topic]])
```

You can see that data will flow from the `:input-topic` to the `:increment-stream`, then from `:increment-stream` to the `:output-topic`.

Now we need to tell Willa what exactly the `:input-topic`, `:increment-stream` and `:output-topic` entities are.
To do this, we'll create the entity config map. It looks like this:

```clojure
(def entities
  {:input-topic {:willa.core/entity-type :topic
                 :topic-name "output-topic"
                 :replication-factor 1
                 :partition-count 1
                 :key-serde (serdes.edn/serde)
                 :value-serde (serdes.edn/serde)}
   :increment-stream {:willa.core/entity-type :kstream
                      :willa.core/xform (map (fn [[k v]] [k (inc v)])) ;; Note that the mapping function expects a key-value tuple
                      }
   :output-topic {:willa.core/entity-type :topic
                  :topic-name "output-topic"
                  :replication-factor 1
                  :partition-count 1
                  :key-serde (serdes.edn/serde)
                  :value-serde (serdes.edn/serde)}})
```

That's all the data Willa needs to build your topology! To get our topology up and running, we'll follow these steps:
1. Create a `KStreamsBuilder` object
2. Call the `willa.core/build-workflow!` function, passing it the builder, workflow, and entities
3. Create a `KafkaStreams` object from the builder
4. Call `start` on it

The code looks like this:

```clojure
(def app-config
  {"application.id" "willa-test"
   "bootstrap.servers" "localhost:9092"
   "cache.max.bytes.buffering" "0"})

(defn start! []
  (let [builder   (doto (streams/streams-builder) ;; step 1
                     (w/build-workflow! ;; step 2
                       {:workflow workflow
                        :entities entities
                        :joins joins}))
         kstreams-app (streams/kafka-streams builder app-config) ;; step 3
         ]
    (streams/start kstreams-app) ;; step 4
    kstreams-app))
```

You can verify that it works by running the following commands in your repl:

```clojure
(require 'jackdaw.client
         'jackdaw.admin
         'willa.streams)

(def admin-client (jackdaw.admin/->AdminClient app-config))       
;; create the input and output topics
(jackdaw.admin/create-topics! admin-client [(:input-topic entities) (:output-topic entities)])

;; start the topology
(def kstreams-app (start!))

;; create a Kafka Producer, and produce a message with value 1 to the input topic
(def producer (jackdaw.client/producer app-config
                                       willa.streams/default-serdes))                           
@(jackdaw.client/send! producer (jackdaw.data/->ProducerRecord (:input-topic entities) "key" 1))  
                                     
;; create a Kafka Consumer, and consume everything from the output topic                                     
(def consumer (jackdaw.client/consumer (assoc app-config "group.id" "consumer")
                                              willa.streams/default-serdes))
(jackdaw.client/subscribe consumer [(:output-topic entities)])
(jackdaw.client/seek-to-beginning-eager consumer)
;; should return something like: [{:key "key" :value 2}] 
(->> (jackdaw.client/poll consumer 200)
     (map #(select-keys % [:key :value])))                                           
```

## License

This program and the accompanying materials are made available under the
terms of the GPL V3 license, which is available at https://www.gnu.org/licenses/gpl-3.0.en.html.
