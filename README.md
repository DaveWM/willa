# willa [![CircleCI](https://circleci.com/gh/DaveWM/willa.svg?style=svg)](https://circleci.com/gh/DaveWM/willa)
**Alpha**


Willa provides a data-driven DSL on top of the [Kafka Streams DSL](https://docs.confluent.io/current/streams/developer-guide/dsl-api.html).

It is named after [Willa Muir](https://en.wikipedia.org/wiki/Willa_Muir), who translated Kafka's "The Metamorphosis".

## Rationale

The Kafka Streams DSL is very "Javaish". 
It uses a `KStreamsBuilder` object to build topologies, which operates by in-place mutation.
This has all the [usual disadvantages](https://clojure.org/about/state#_object_oriented_programming_oo) of mutability, 
including making topologies more difficult to compose, test, and visualise. 
It also unnecessarily re-implements many functions from the core Clojure library, for example `map` and `filter`. 

Willa aims to provide an immutable, data-driven DSL on top of the Kafka Streams DSL. 
It represents the topologyThis has several nice advantages:
1. It makes topologies far easier to compose. For example, if you want to log every message that is published to output topics,
you can write a generic pure function to transform a topology (represented as data) to acheive this.   
 

## License

This program and the accompanying materials are made available under the
terms of the GPL V3 license, which is available at https://www.gnu.org/licenses/gpl-3.0.en.html.
