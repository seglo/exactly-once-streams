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
import scala.util.Try

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

  val CONSUMER_POLL_TIMEOUT = 200L

  val consumer = new KConsumer(consumerProps, new StringDeserializer, new StringDeserializer)
  consumer.subscribe(List(appSettings.dataSinkTopic).asJava)

  case class SinkState(records: List[(Int, Int, Int)], partitions: Int, messagesPerPartition: Int) {

    def assert(): Boolean = {
      import org.scalatest._
      import Matchers._

      val assertions = (0 until partitions).map { partition =>
        val expectedPartitionRecords = (0 until messagesPerPartition).map { counter =>
          (partition, counter, counter * 2)
        }
        Try {
          /**
            * Asserts that the generated expected records occur in order in the actual records.  Because Kafka only
            * guarantees message order within a partition we can only assert order per partition.  ScalaTest has a nice
            * collections matcher to help here.
            *
            * ScalaTest will return an `Assertion` `Succeeded` type when a matcher passes and a `TestFailedException`
            * on failure.
            *
            * http://doc.scalatest.org/3.0.0/index.html#org.scalatest.Matchers@inOrderElementsOf[R](elements:scala.collection.GenTraversable[R]):org.scalatest.words.ResultOfInOrderElementsOfApplication
            */
          records should contain inOrderElementsOf expectedPartitionRecords
        }
      }

      assertions.forall(_.isSuccess)
    }
  }
  val stateSeed = SinkState(List[(Int,Int,Int)](), appSettings.partitionCount, appSettings.messagesPerPartition)

  Stream.continually(consumer.poll(CONSUMER_POLL_TIMEOUT))
    .takeWhile(_ ne null)
    .filterNot(_.isEmpty)

    /**
      * This is something that akka-streams would do well: accumulate state until some condition, then reset state.
      * For our purposes we will just restart the the AssertSink app
      * See an example here:
      * http://doc.akka.io/docs/akka/snapshot/scala/stream/stream-cookbook.html#calculating-the-digest-of-a-bytestring-stream
      */
    .scanLeft(stateSeed) { case (sinkState, consumerRecords) =>
      val records = consumerRecords.iterator().asScala.map { record =>
        println(s"Partition ${record.partition()}, Key ${record.key()}, Value: ${record.value()}")
        (record.partition(), record.key().toInt, record.value().toInt)
      }
      sinkState.copy(records = sinkState.records ++ records)
    }
    .find(_.assert())
    .foreach { _ =>
      println("Assertion succeeded!")
    }
}
