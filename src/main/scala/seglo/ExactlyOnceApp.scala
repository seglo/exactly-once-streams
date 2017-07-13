package seglo

import akka.stream._
import akka.stream.scaladsl._
import akka.{Done, NotUsed}
import akka.actor.ActorSystem
import akka.util.ByteString

import scala.concurrent._
import scala.concurrent.duration._
import java.nio.file.Paths
import java.util.Properties

import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringSerializer
/**
  * Created by seglo on 13/07/17.
  */
case class AppSettings(
                        dataSourceTopic: String = "datasource",
                        bootstrapServers: String = "localhost:9092"
                      )

object ExactlyOnceApp extends App {
  type K = String
  type V = String
  type KRecord = ProducerRecord[K, V]

  case class KResult[M](metadata: RecordMetadata, msg: M)

  implicit val system = ActorSystem("ExactlyOnceApp")
  implicit val materializer = ActorMaterializer()

  val appSettings = AppSettings()
  val properties: Properties = {
    val p = new Properties()
    p.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer])
    p.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
    p
  }
  val producer = new KafkaProducer[K, V](properties, new StringSerializer, new StringSerializer)

  val idempotentProducerFlow: Flow[KRecord, Future[KResult[KRecord]], NotUsed] =
    Flow[KRecord].map { message =>
      println(s"Generated: $message")

      val r = Promise[KResult[KRecord]]
      producer.send(message, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception) = {
          if (exception == null)
            r.success(KResult(metadata, message))
          else
            r.failure(exception)
        }
      })

      r.future
    }
  //source.runForeach(i => println(i))(materializer)

  val graph = Source(1 to 100)
      .map { n =>
        new KRecord(appSettings.dataSourceTopic, "key", n.toString)
      }
      .via(idempotentProducerFlow)
      .toMat(Sink.ignore)(Keep.right)
      .run()
}
