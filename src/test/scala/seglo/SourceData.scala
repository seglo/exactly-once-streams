package seglo

import java.util.Properties

import akka.Done
import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import akka.stream.scaladsl.{Flow, Keep, Sink, Source}
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import seglo.apps.{AppSettings, KProducer, KProducerRecord, KResult}

import scala.concurrent.{Future, Promise}

object SourceData {
  def populate(appSettings: AppSettings): Future[Done] = {
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
      //p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "my-transactional-id")
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
    val graph: Future[Done] = Source(0 until appSettings.messagesPerPartition)
      .mapConcat { n =>
        (0 until appSettings.partitions).flatMap { partitionNumber =>
          val start = startMessage(n, partitionNumber)
          val end = endMessage(n, partitionNumber)

          start ++
            Seq(new KProducerRecord(appSettings.dataSourceTopic, partitionNumber, n.toString, n.toString)) ++
            end
        }.toList
      }
      .runWith(plainSink(producer))

    graph.onComplete { _ =>
      producer.close()
      system.terminate()
    }

    graph
  }

  def plainSink(producer: KProducer): Sink[KProducerRecord, Future[Done]] = {

    Flow[KProducerRecord].mapAsync(1) { message =>
      println(s"Producing: $message")

      val r = Promise[KResult[KProducerRecord]]
      producer.send(message, new Callback {
        override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
          if (exception == null) {
            println(s"Successfully produced: $message")
            r.success(KResult(metadata, message))
          } else {
            println(s"Failed to produce: $message")
            println(exception)
            r.failure(exception)
          }
        }
      })

      r.future
    }
    .toMat(Sink.ignore)(Keep.right)
  }
}
