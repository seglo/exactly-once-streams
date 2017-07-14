# Exactly Once Streaming in Kafka 0.11.0.0

## 1) Background

Exactly once delivery is considered by many as a holy grail in terms of message delivery semantics in distributed
systems.  To the uninitiated, exactly once sounds like a reasonable requirement for any system, but the implications
in achieving these guarantees are glossed over.  I'll provide a brief review of message delivery semantics before we
evaluate these implications in detail.

### Message Delivery Semantics

There are 3 types of message delivery semantics: At Most Once, At Least Once, and Exactly Once.  These semantics
describe how messages are delivered to a destination when accounting for common failure use cases such as network
partitions, producer (source) failure, consumer (sink, application processor) failure, etc.

- At Most Once: Messages are only ever sent to their destination once.  They are either received successfully or they 
are not.  If they are not received due to a failure then the downstream sink will have gaps in its message log.  At most
once can be advantageous because of the performance benefits involved.  Processes such as acknowledgement of receipt,
write consistency guarantees, retries, are add additional overhead and latency.  If the stream can tolerate some failure
and requires very low latency to process at a high volume, then this may be acceptable.
- At Least Once: Messages are sent to their destination.  An acknowledgement is required so the sender knows the 
message was received.  In the event of failure the source can retry to send the message.  In this situation it's 
possible to have 1 or more duplicates at the sink.  Sink systems may be tolerant of this by ensuring they persist
messages in an idempotent way.  This is the most common compromise between at most once and exactly once semantics.
- Exactly Once: Messages are sent to their destination once and only once.  The sink will only process the message once.
Messages will only arrive in the order they're sent.  This sounds good.  Those uninformed about the consequences often 
lean towards exactly once because it's the best kind of guarantee you can think of.  Unfortunately, depending on how you 
define exactly once it can be either difficult or impossible to achieve.

### "You Cannot Have Exactly Once Delivery"

Distributed systems engineers agree that exactly once delivery is impossible.  This view was popularized by an engineer
named Tyler Treat who wrote a famous blog post called "You Cannot Have Exactly Once Delivery".  This post
highlights a thought experiment known as the Two Generals problem (related to the more general Byzantine 
Generals Problem).  Summarized below, emphasis mine.

> In computing, the Two Generals Problem is a thought experiment meant to illustrate the pitfalls and design challenges of attempting to coordinate an action by communicating over an unreliable link. It is related to the more general Byzantine Generals Problem (though published long before that later generalization) and appears often in introductory classes about computer networking (particularly with regard to the Transmission Control Protocol where it shows that **TCP can't guarantee state consistency between endpoints** and why), though it applies to any type of two party communication where failures of communication are possible.

*[Two Generals Problem](https://en.wikipedia.org/wiki/Two_Generals%27_Problem)*

After te Kafka 0.11.0.0 caused some debate in the community, Tyler has clarified his position in a new post: 
[You Cannot Have Exactly-Once Delivery Redux](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery-redux/).

### Exactly Once Delivery vs Processing

You may ask if exactly once delivery is impossible then how can companies claim they support it.  The answer is really
all in the semantics used (no pun intended) to describe the process.  As described in the summary of the Two Generals
Problem, exactly once is an impossibility, but only within reference to the network transport layer (TCP's transport
layer).  The way that Kafka and other stream processing technologies implement exactly once is by applying it at the 
application layer.  **The message delivery workflow can be augmented to perform additional operations at the application
processing layer that fake exactly once delivery guarantees**.  So instead of calling it exactly-once message delivery, 
let's expand the definition to **Exactly Once Processing at the Application layer**.

Faking Exactly Once Processing in its most basic form is at least once message delivery with effective idempotency
guarantees on the sink.  The following operations are required to make this work.

- Retry until acknowledgement from sink
- Idempotent data sources on the receiving side.  Persist received messages to an idempotent data store that will ensure
no duplicates.
- Application deduplication logic.  Achieve effective idempotency by deduplicating messages based on your own 
application's requirements.

### Kafka and Exactly Once

**Kafka released version 0.11.0.0 on June 28th, 2017.** Exactly once delivery on the Kafka producer was 
impossible to guarantee before the release of 0.11.0.0 because there was a possibility that a producer could send a 
duplicate message in a retry and fail before it received an acknowledgement.

Kafka achieves Exactly Once Processing by making a number of changes to Kafka broker and client code.

* Idempotent producers.  When `ENABLE_IDEMPOTENCE` is enabled in the producer config it will coordinate with the broker
to ensure that retried messages aren't duplicated.
* Transaction in `consume->transform->produce` flow.  Transaction have been introduced to guarantee exactly once in a
`consume->transform->produce` flow.  This is a common operation involving consuming from a Kafka topic, processing, and
producing to a new topic.  To make transactions a reality in Kafka 0.11 the following changes were made:
  * Idempotent producers
  * Transaction Coordinator: New logical component on broker
  * Message schema changes: New concepts such as Transaction ID, Producer ID, and transaction sequence numbers.

#### Transaction Key Concepts

```
To implement transactions, ie. ensuring that a group of messages are produced and consumed atomically, we introduce several new concepts:

1) We introduce a new entity called a Transaction Coordinator. Similar to the consumer group coordinator, each producer is assigned a transaction coordinator, and all the logic of assigning PIDs and managing transactions is done by the transaction coordinator.
2) We introduce a new internal kafka topic called the Transaction Log. Similar to the Consumer Offsets topic, the transaction log is a persistent and replicated record of every transaction. The transaction log is the state store for the transaction coordinator, with the snapshot of the latest version of the log encapsulating the current state of each active transaction.
3) We introduce the notion of Control Messages. These are special messages written to user topics, processed by clients, but never exposed to users. They are used, for instance, to let brokers indicate to consumers if the previously fetched messages have been committed atomically or not. Control messages have been previously proposed here.
4) We introduce a notion of TransactionalId, to enable users to uniquely identify producers in a persistent way. Different instances of a producer with the same TransactionalId will be able to resume (or abort) any transactions instantiated by the previous instance.
5) We introduce the notion of a producer epoch, which enables us to ensure that there is only one legitimate active instance of a producer with a given TransactionalId, and hence enables us to maintain transaction guarantees in the event of failures.

In additional to the new concepts above, we also introduce new request types, new versions of existing requests, and new versions of the core message format in order to support transactions. The details of all of these will be deferred to other documents.
```
*[KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)*

#### `consumer->transform->produce` Sample app

```
public class KafkaTransactionsExample {
  
  public static void main(String args[]) {
    KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumerConfig);
 
 
    // Note that the ‘transactional.id’ configuration _must_ be specified in the
    // producer config in order to use transactions.
    KafkaProducer<String, String> producer = new KafkaProducer<>(producerConfig);
 
    // We need to initialize transactions once per producer instance. To use transactions,
    // it is assumed that the application id is specified in the config with the key
    // transactional.id.
    //
    // This method will recover or abort transactions initiated by previous instances of a
    // producer with the same app id. Any other transactional messages will report an error
    // if initialization was not performed.
    //
    // The response indicates success or failure. Some failures are irrecoverable and will
    // require a new producer  instance. See the documentation for TransactionMetadata for a
    // list of error codes.
    producer.initTransactions();
     
    while(true) {
      ConsumerRecords<String, String> records = consumer.poll(CONSUMER_POLL_TIMEOUT);
      if (!records.isEmpty()) {
        // Start a new transaction. This will begin the process of batching the consumed
        // records as well
        // as an records produced as a result of processing the input records.
        //
        // We need to check the response to make sure that this producer is able to initiate
        // a new transaction.
        producer.beginTransaction();
         
        // Process the input records and send them to the output topic(s).
        List<ProducerRecord<String, String>> outputRecords = processRecords(records);
        for (ProducerRecord<String, String> outputRecord : outputRecords) {
          producer.send(outputRecord);
        }
         
        // To ensure that the consumed and produced messages are batched, we need to commit
        // the offsets through
        // the producer and not the consumer.
        //
        // If this returns an error, we should abort the transaction.
         
        sendOffsetsResult = producer.sendOffsetsToTransaction(getUncommittedOffsets());
         
      
        // Now that we have consumed, processed, and produced a batch of messages, let's
        // commit the results.
        // If this does not report success, then the transaction will be rolled back.
        producer.endTransaction();
      }
    }
  }
}
```
*[KIP-98](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)*

## 2) Hypothesis

> Exactly once message processing is possible in Kafka for a consume->transform->produce operation

The Exactly once debate and semantics between exactly once at a network and application layer.  See Background section
for more details.

## 3) Details of the experiment

* Kafka 0.11.0.0 Brokers
  * Used a fork of the popular [wurstmeister/kafka](https://hub.docker.com/r/wurstmeister/kafka/) Kafka docker image to
  host Kafka brokers.
    * [Pending PR with support for Kafka 0.11.0.0](https://github.com/wurstmeister/kafka-docker/pull/200)
  * `docker-compose` used to orchestrate local Kafka image build and [wurstmeister/zookeeper]((https://hub.docker.com/r/wurstmeister/zookeeper/))
* Three Scala Applications.  All using new Kafka client library version.
  * `FillSourceTopic`
    * Uses akka-streams with custom Kafka sink for Idempotent Producer.
    * Produces integers to `datasource` topic using `StringSerializer`.  Partitions: 3, Key: String (int), Value: String (int)
  * `ConsumeTransformProduce`
    * Scala Streams implementation of a simple `consume->transform->produce` workflow with the new transactional support.
    * *I wanted to use reactive-kafka here, which marries together akka-streams and Kafka clients, but did not have time
     to upgrade reactive-kafka to support new Kafka release.*
    * Consumes from `datasource` topic.  Transforms elements by applying square operation to integers.
    * Produces to `datasink` topic.
    * **Support for 0..1 probability of transformation transaction failing.  This is to demonstrate common failure scenarios
    that require the transaction to be aborted and processing to be reset.  See `AppSettings.transactionFailureProbability`
    configuration.**
  * `AssertSinkTopic`
    * Scala Streams implementation of a `READ_COMMITTED` consumer.
    * Asserts the following properties on messages in `datasink`:
      * No duplicate messages
      * Ordered within a partition
      * No elements are missing
    * Uses ScalaTest Matchers
    
## 4. Results

* Preliminary results of Kafka's Exactly Once Processing features can handle transaction failure nicely.
* 

5. Validated Learning

* Exactly Once semantics for producing messages were not possible in versions of Kafka < 0.11.0.0
* Kafka transactions add overhead to stream processing layer.  By applying transactions to subsets of partitions it is
possible to increase processing performance.
* Transactional support for `consume->transform->produce` workflow in `reactive-kafka` could require significant
effort to support.
* It's the responsibility of the user to manage stateful streaming data in the event of an aborted transaction.
  * This can be supported by Kafka Streams.
  * Flink has a compelling story for exactly-once and stream state management that pre-dates Kafka.  See snapshot and
  checkmarking features.

6. Next Action

- Re-factor as ScalaTest integration test.  Embedded Kafka or integrated docker-compose.
- Durable snapshots for streaming state
  - See Kafka Streams, Flink for reference
- Performance testing and comparison to at-least-once workflows
    - Transactional support limited to single consumer and producer which can limit throughput
    - Transactional support adds overhead and latency due to requirements
        - Producer acks=all
        - Coordination costs with transaction coordinator
        - `READ_COMMITTED` consumer lag
- Create PR for reactive-kafka
    - Support idempotent producer for `Producer.plainSink`
    - Support transactions for `Consumer.committableSink`

# Resources

* Kafka Improvement Proposals (KIP) for Transaction Support
  * [KIP-98 - Exactly Once Delivery and Transactional Messaging](https://cwiki.apache.org/confluence/display/KAFKA/KIP-98+-+Exactly+Once+Delivery+and+Transactional+Messaging)
  * [KIP-129: Streams Exactly-Once Semantics](https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics)
* [You Cannot Have Exactly-Once Delivery](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery/)
* [You Cannot Have Exactly-Once Delivery Redux](http://bravenewgeek.com/you-cannot-have-exactly-once-delivery-redux/)
* [Two Generals' Problem](https://en.wikipedia.org/wiki/Two_Generals%27_Problem)
* [Exactly-once Semantics are Possible: Here’s How Kafka Does it](https://www.confluent.io/blog/exactly-once-semantics-are-possible-heres-how-apache-kafka-does-it/)
* [Apache Kafka 0.11.0.0 Documentation](https://kafka.apache.org/documentation/)
* [Working with Sequences in ScalaTest](http://www.scalatest.org/user_guide/using_matchers#workingWithSequences)
* [reactive-kafka AKA Akka Streams Kafka](http://doc.akka.io/docs/akka-stream-kafka/current/home.html)
* [Using cats with reactive-kafka](https://www.iravid.com/posts/using-cats-with-reactive-kafka.html)
  * Use cats FP library to reduce boilerplate in akka-streams implementation with reactive-kafka
* [Apache Flink](https://flink.apache.org/)
