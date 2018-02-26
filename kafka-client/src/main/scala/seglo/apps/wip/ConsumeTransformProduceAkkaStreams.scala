//package seglo.apps.wip
//
//import java.util.{Locale, Properties}
//
//import akka.actor.ActorSystem
//import akka.stream.ActorMaterializer
//import akka.stream.scaladsl.{Sink, Source}
//import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
//import org.apache.kafka.clients.producer.{Callback, ProducerConfig, RecordMetadata}
//import org.apache.kafka.common.TopicPartition
//import org.apache.kafka.common.requests.IsolationLevel
//import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
//import seglo.AppSettings
//import seglo.apps._
//
//import scala.collection.JavaConverters._
//import scala.collection.mutable
//import scala.concurrent.Promise
//
////import cats.implicits._
//
//object ConsumeTransformProduceAkkaStreams extends App {
//
//  def getUncommittedOffsets(consumer: KConsumer) : java.util.Map[TopicPartition, OffsetAndMetadata]  = {
//    val offsetsToCommit = new mutable.HashMap[TopicPartition, OffsetAndMetadata]()
//    consumer.assignment.forEach { topicPartition =>
//      offsetsToCommit.put(topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
//    }
//    offsetsToCommit.toMap.asJava
//  }
//
//  implicit val system = ActorSystem("ExactlyOnceApps")
//  implicit val materializer = ActorMaterializer()
//  implicit val dispatcher = system.dispatcher
//
//  val appSettings = AppSettings()
//
//  val producerProps: Properties = {
//    val p = new Properties()
//    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
//    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
//    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
//    /**
//      * Producer Idempotence
//      *
//      * Required to enable exactly-once delivery of messages when producing messages to Kafka.  A more accurate way
//      * to describe this config may be "at-least-once delivery to a topic that handles deduplication".  This config
//      * will override default values of `max.in.flight.requests.per.connection=1` and `acks=all`.  See docs for details.
//      * https://kafka.apache.org/documentation/#producerconfigs
//      */
//    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true.toString)
//    /**
//      * Transactional ID
//      *
//      * Used to enforce that this producer completes any running transactions before a producer with the same
//      * transactional ID can create a new transaction.
//      */
//    p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ConsumeTransformProduceApp")
//    p
//  }
//
//  val consumerProps: Properties = {
//    val p = new Properties()
//    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
//    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
//    p.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumeTransformProduceApp")
//    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
//    /**
//      * Consumer Transaction Isolation Level
//      *
//      * Required to ensure that the consumer only reads committed messages from the topic.
//      */
//    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ROOT))
//    p
//  }
//
//  val CONSUMER_POLL_TIMEOUT = 60000L
//
//  //type TransactionRecord[T] = (TxContext, T)
//  case class TxContext(producer: KProducer, consumer: KConsumer) {
//    def consumerPoll() =
//      TxPayload(this, consumer.poll(CONSUMER_POLL_TIMEOUT))
//
//    def close(): Unit = {
//      consumer.close()
//      producer.close()
//    }
//  }
//
//  case class TxPayload[T](context: TxContext, payload: T)
//
//  Source.unfoldResource[TxPayload[KConsumerRecords], TxContext](
//    // create resource
//    () => {
//      val producer = new KProducer(producerProps)
//      val consumer = new KConsumer(consumerProps, new StringDeserializer, new StringDeserializer)
//      consumer.subscribe(List(appSettings.dataSourceTopic).asJava)
//      producer.initTransactions()
//      TxContext(producer, consumer)
//    },
//    // read resource (TransactionContext) on upstream demand
//    (context) => Option(context.consumerPoll()),
//    // close resource (TransactionContext) when read operation returns None
//    (context) => context.close()
//  )
//    .filterNot { case TxPayload(_, records) => records.isEmpty }
//    .map { case txp @ TxPayload(context, _) =>
//      // Start a new transaction. This will begin the process of batching the consumed records as well as an records
//      // produced as a result of processing the input records.
//
//      // We need to check the response to make sure that this producer is able to initiate a new transaction.
//      context.producer.beginTransaction()
//      txp
//    }
//    .mapConcat { txPayload =>
//      // Disaggregate ConsumerRecords.  Pass along TxContext for each ConsumerRecord.
//      txPayload.payload.iterator().asScala
//        .map(record => txPayload.copy(payload = record))
//        .toList
//    }
//    .map { case TxPayload(context, record) =>
//      // Transform
//      val squared = record.value().toInt * 2
//
//      // Produce transformed records to sink topic
//      val producerRecord = new KProducerRecord(appSettings.dataSinkTopic, record.key(), squared.toString)
//      println(s"Producing: $producerRecord")
//      val r = Promise[KResult[KProducerRecord]]
//      context.producer.send(producerRecord, new Callback {
//        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
//          if (exception == null) {
//            println(s"Successfully produced: $producerRecord")
//            r.success(KResult(metadata, producerRecord))
//          } else {
//            println(s"Failed to produce: $producerRecord")
//            println(exception)
//            r.failure(exception)
//          }
//        }
//      })
//
//      //r.future.map(result => TxPayload(context, result))
//      TxPayload(context, r.future)
//    }
//    .map { case TxPayload(context, _) =>
//      // To ensure that the consumed and produced messages are batched, we need to commit the offsets through the
//      // producer and not the consumer.  If this returns an error, we should abort the transaction.
//      try {
//        println("Sending offsets to Transaction Coordinator...")
//        context.producer.sendOffsetsToTransaction(
//          getUncommittedOffsets(context.consumer),
//          consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
//        )
//      } catch {
//        case _: Exception => context.producer.abortTransaction()
//      }
//
//      // Now that we have consumed, processed, and produced a batch of messages, let's commit the results.
//      // If this does not report success, then the transaction will be rolled back.
//      context.producer.commitTransaction()
//    }
//    .runWith(Sink.ignore)
//
//
////  while (true) {
////    val records: ConsumerRecords[K, V] = consumer.poll(CONSUMER_POLL_TIMEOUT)
////    if (!records.isEmpty) {
////      // Start a new transaction. This will begin the process of batching the consumed records as well
////      // as an records produced as a result of processing the input records.
////      //
////      // We need to check the response to make sure that this producer is able to initiate a new transaction.
////      producer.beginTransaction()
////
////      // Process the input records and send them to the output topic(s).
////      val outputRecords = records.iterator().asScala
////        .map { cr =>
////          val squared = cr.value().toInt * 2
////          new KProducerRecord(appSettings.dataSinkTopic, cr.key(), squared.toString)
////        }
////
////      for (outputRecord <- outputRecords) {
////        producer.send(outputRecord)
////      }
////
////      // To ensure that the consumed and produced messages are batched, we need to commit the offsets through the
////      // producer and not the consumer.  If this returns an error, we should abort the transaction.
////      try {
////        producer.sendOffsetsToTransaction(
////          getUncommittedOffsets(consumer),
////          consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
////        )
////      } catch {
////        case _: Exception => producer.abortTransaction()
////      }
////
////      // Now that we have consumed, processed, and produced a batch of messages, let's
////      // commit the results.
////      // If this does not report success, then the transaction will be rolled back.
////      producer.commitTransaction()
////    }
////  }
//}
