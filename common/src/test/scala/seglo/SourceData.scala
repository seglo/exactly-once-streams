package seglo

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.producer.{Callback, ProducerConfig, RecordMetadata}
import org.apache.kafka.common.serialization.StringSerializer
import seglo.apps._

import scala.concurrent.duration.{FiniteDuration, MINUTES}
import scala.concurrent.{Await, Future, Promise}

object SourceData extends LazyLogging {
  val timeout = FiniteDuration(1, MINUTES)

  def populate(appSettings: AppSettings): Unit = {

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
        Seq(new KProducerRecord(appSettings.dataSourceTopic, p, s"P$p-START", "START"))
      else Nil

    val endMessage = (n: Int, p: Int) =>
      if (n == appSettings.messagesPerPartition - 1)
        Seq(new KProducerRecord(appSettings.dataSourceTopic, p, s"P$p-END", "END"))
      else Nil

    val producer = new KProducer(properties, new StringSerializer, new StringSerializer)

    val results: Seq[Future[KResult[KProducerRecord]]] = (0 until appSettings.messagesPerPartition)
      .flatMap { n =>
        (0 until appSettings.partitions).flatMap { partitionNumber =>
          val start = startMessage(n, partitionNumber)
          val end = endMessage(n, partitionNumber)

          start ++
            Seq(new KProducerRecord(appSettings.dataSourceTopic, partitionNumber, s"P$partitionNumber-$n", n.toString)) ++
            end
        }.toList
      }
      .map { message =>
        logger.info(s"Producing: $message")

        val r = Promise[KResult[KProducerRecord]]
        producer.send(message, new Callback {
          override def onCompletion(metadata: RecordMetadata, exception: Exception): Unit = {
            if (exception == null) {
              logger.info(s"Successfully produced: $message")
              r.success(KResult(metadata, message))
            } else {
              logger.error(s"Failed to produce: $message", exception)
              r.failure(exception)
            }
          }
        })

        r.future
      }

    import scala.concurrent.ExecutionContext.Implicits.global

    Await.result(Future.sequence(results), timeout)
    producer.close()
  }
}
