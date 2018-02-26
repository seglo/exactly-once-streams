package seglo

import java.util.{Locale, Properties}

import akka.actor.ActorSystem
import akka.kafka.{ConsumerSettings, ProducerMessage, ProducerSettings, Subscriptions}
import akka.kafka.scaladsl.{Consumer, Producer}
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Sink, Source}
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, ProducerRecord, RecordMetadata}
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{ByteArrayDeserializer, StringDeserializer, StringSerializer}
import seglo.apps._

import scala.collection.JavaConverters._
import scala.collection.mutable
import scala.concurrent.{ExecutionContextExecutor, Promise}

//import cats.implicits._

object ConsumeTransformProduceAkkaStreams extends App {

  def getUncommittedOffsets(consumer: KConsumer): java.util.Map[TopicPartition, OffsetAndMetadata] = {
    consumer.assignment.asScala.map { topicPartition =>
      (topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
    }.toMap.asJava
  }

  implicit val system: ActorSystem = ActorSystem("ReactiveKafkaExactlyOnce")
  implicit val materializer: ActorMaterializer = ActorMaterializer()
  implicit val dispatcher: ExecutionContextExecutor = system.dispatcher

  val appSettings = AppSettings()

  val producerSettings = ProducerSettings(system, new StringSerializer, new StringSerializer)
    .withBootstrapServers(appSettings.bootstrapServers)
    .withProperties(
      /**
        * Producer Idempotence
        *
        * Required to enable exactly-once delivery of messages when producing messages to Kafka.  A more accurate way
        * to describe this config may be "at-least-once delivery to a topic that handles deduplication".  This config
        * will override default values of `max.in.flight.requests.per.connection=1` and `acks=all`.  See docs for details.
        * https://kafka.apache.org/documentation/#producerconfigs
        */
      ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG -> true.toString,
      /**
        * Transactional ID
        *
        * Used to enforce that this producer completes any running transactions before a producer with the same
        * transactional ID can create a new transaction.
        */
      ProducerConfig.TRANSACTIONAL_ID_CONFIG -> "ConsumeTransformProduceApp"
    )

  val consumerSettings = ConsumerSettings(system, new StringDeserializer, new StringDeserializer)
    .withBootstrapServers(appSettings.bootstrapServers)
    .withGroupId("ConsumeTransformProduceApp")
    .withProperties(
      ConsumerConfig.AUTO_OFFSET_RESET_CONFIG -> "earliest",
      /**
        * Consumer Transaction Isolation Level
        *
        * Required to ensure that the consumer only reads committed messages from the topic.
        */
      ConsumerConfig.ISOLATION_LEVEL_CONFIG -> IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ROOT)
    )

  Consumer.committableSource(consumerSettings, Subscriptions.topics(appSettings.dataSourceTopic))
    .map { msg =>
      println(s"topic1 -> topic2: $msg")
      ProducerMessage.Message(new ProducerRecord[String, String](
        "topic2",
        msg.record.value
      ), msg.committableOffset)
    }
    .runWith(Producer.commitableSink(producerSettings))

}
