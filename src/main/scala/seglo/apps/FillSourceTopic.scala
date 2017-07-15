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
  implicit val dispatcher = system.dispatcher

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

  val startMessage = (n: Int, p: Int) =>
    if (n == 0)
      Seq(new KProducerRecord(appSettings.dataSourceTopic, p, "START", "START"))
    else Nil

  val endMessage = (n: Int, p: Int) =>
    if (n == appSettings.messagesPerPartition - 1)
      Seq(new KProducerRecord(appSettings.dataSourceTopic, p, "END", "END"))
    else Nil

  val producer = new KProducer(properties, new StringSerializer, new StringSerializer)

  val lastMessage = appSettings.messagesPerPartition - 1
  val graph = Source(0 until appSettings.messagesPerPartition)
      .mapConcat { n =>
        (0 until appSettings.partitionCount).flatMap { partitionNumber =>
          val start = startMessage(n, partitionNumber)
          val end = endMessage(n, partitionNumber)

          start ++
            Seq(new KProducerRecord(appSettings.dataSourceTopic, partitionNumber, n.toString, n.toString)) ++
            end
        }.toList
      }
      .runWith(seglo.impl.Producer.plainSink(producer))
      .onComplete { _ =>  system.terminate() }
}
