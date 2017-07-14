package seglo.apps

import java.util.Properties

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl._
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer

import seglo.impl._

/**
  * Fill the data source topic with an idempotent producer.
  */
object FillSourceTopic extends App {
  implicit val system = ActorSystem("ExactlyOnceApps")
  implicit val materializer = ActorMaterializer()

  val appSettings = AppSettings()
  val properties: Properties = {
    val p = new Properties()
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
    /**
      * Producer Idempotence
      *
      * Required to enable exactly-once delivery of messages when producing messages to Kafka.  A more accurate way
      * to describe this config may be "at-least-once delivery to a topic that handles deduplication".  This config
      * will override default values of `max.in.flight.requests.per.connection=1` and `acks=all`.  See docs for details.
      * https://kafka.apache.org/documentation/#producerconfigs
      */
    p.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true.toString)
    p
  }
  val producer = new KProducer(properties, new StringSerializer, new StringSerializer)

  val graph = Source(1 to 100)
      .mapConcat { n =>
        List(
          new KProducerRecord(appSettings.dataSourceTopic, 0, null, n.toString),
          new KProducerRecord(appSettings.dataSourceTopic, 1, null, n.toString),
          new KProducerRecord(appSettings.dataSourceTopic, 2, null, n.toString)
        )
      }
      .runWith(seglo.impl.Producer.plainSink(producer))
}
