package seglo.apps

import java.util.{Locale, Properties}

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.StringDeserializer
import seglo.apps.ConsumeTransformProduce.{CONSUMER_POLL_TIMEOUT, appSettings, consumer, producer, producerProps}
import seglo.impl.{KConsumer, KConsumerRecords, KProducer}

import scala.collection.JavaConverters._

object AssertSinkTopic extends App {
  implicit val system = ActorSystem("ExactlyOnceApps")
  implicit val materializer = ActorMaterializer()
  implicit val dispatcher = system.dispatcher

  val appSettings = AppSettings()

  val consumerProps: Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "ConsumeTransformProduceApp")
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
    /**
      * Consumer Transaction Isolation Level
      *
      * Required to ensure that the consumer only reads committed messages from the topic.
      */
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ROOT))
    p
  }

  val CONSUMER_POLL_TIMEOUT = 60000L

  val consumer = new KConsumer(consumerProps, new StringDeserializer, new StringDeserializer)
  consumer.subscribe(List(appSettings.dataSinkTopic).asJava)

  Stream.continually(consumer.poll(CONSUMER_POLL_TIMEOUT))
    .takeWhile(_ ne null)
    .filterNot(_.isEmpty)
    .foreach { consumerRecords =>
      consumerRecords.iterator().asScala.foreach(println)
    }
}
