package seglo.apps

import java.util.{Locale, Properties}

import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.StringDeserializer
import seglo.impl.KConsumer

import scala.collection.JavaConverters._
import scala.util.Try

object AssertSinkTopic extends App {
  val appSettings = AppSettings()

  val consumerProps: Properties = {
    val p = new Properties()
    p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
    p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
    p.put(ConsumerConfig.GROUP_ID_CONFIG, "AssertSinkTopic")
    p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
    /**
      * Consumer Transaction Isolation Level
      *
      * Required to ensure that the consumer only reads committed messages from the topic.
      */
    p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
    p
  }

  println(consumerProps)

  val CONSUMER_POLL_TIMEOUT = 200L

  val consumer = new KConsumer(consumerProps, new StringDeserializer, new StringDeserializer)
  consumer.subscribe(List(appSettings.dataSinkTopic).asJava)

  case class SinkState(records: List[(Int, String, String)], partitions: Int, messagesPerPartition: Int) {

    val controlMessages = 2

    def assert(): Boolean = {
      println("Assert begin")

      import org.scalatest._
      import Matchers._

      /**
        * ScalaTest will return an `Assertion` `Succeeded` type when a matcher passes and throw a `TestFailedException`
        * on failure.
        */
      Try {
        /**
          * There should be exactly messagesPerPartition * partitionCount number of records
          */
        println(s"Records length ${records.length}")
        records.length should equal ((appSettings.messagesPerPartition + controlMessages) * appSettings.partitionCount)

        val byPartition = records.groupBy { case (partition, _, _) => partition }

        /**
          * There should be exactly partitionCount number of partitions
          */
        println(s"Partitions ${byPartition.size}")
        byPartition.size should equal (appSettings.partitionCount)

        /**
          * Each partition should have exactly messagesPerPartition number of messages
          */
        byPartition.map { case (_, partitionMessages) =>
          println(s"Partition messages ${partitionMessages.length}")
          partitionMessages.length should equal (appSettings.messagesPerPartition + controlMessages)
        }

        (0 until partitions).map { partition =>
          val expectedPartitionRecords = (partition, "START", "START_TRANSFORM") +:
            (0 until messagesPerPartition).map { counter =>
              (partition, counter.toString, (counter * 2).toString)
            } :+
            (partition, "END", "END_TRANSFORM")

          Try {
            /**
              * Asserts that the generated expected records occur in order in the actual records.  Because Kafka only
              * guarantees message order within a partition we can only assert order per partition.  ScalaTest has a nice
              * collections matcher to help here.
              *
              * http://doc.scalatest.org/3.0.0/index.html#org.scalatest.Matchers@inOrderElementsOf[R](elements:scala.collection.GenTraversable[R]):org.scalatest.words.ResultOfInOrderElementsOfApplication
              */
            records should contain inOrderElementsOf expectedPartitionRecords
          }
        }
      }.isSuccess
    }
  }

  val stateSeed = SinkState(List[(Int,String,String)](), appSettings.partitionCount, appSettings.messagesPerPartition)

  Stream.continually(consumer.poll(CONSUMER_POLL_TIMEOUT))
    /**
      * This is something that akka-streams would do well: accumulate state until some condition, then reset state.
      * For our purposes we will just restart the the AssertSink app
      * See an example here:
      * http://doc.akka.io/docs/akka/snapshot/scala/stream/stream-cookbook.html#calculating-the-digest-of-a-bytestring-stream
      */
    .scanLeft(stateSeed) { (sinkState, consumerRecords) =>
      println(s"Sink state begin with ${consumerRecords.count()} records")
      val records = consumerRecords.iterator().asScala.map { record =>
        println(s"Partition ${record.partition()}, Key ${record.key()}, Value: ${record.value()}")
        (record.partition(), record.key(), record.value())
      }
      sinkState.copy(records = sinkState.records ++ records)
    }
    .find(_.assert())
    .foreach { _ =>
      println("Assertion succeeded!")
      consumer.close()
    }
}
