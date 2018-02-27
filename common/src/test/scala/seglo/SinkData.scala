package seglo

import java.util.Properties

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer

import scala.collection.JavaConverters._
import scala.util.Try
import seglo.apps._

object SinkData extends LazyLogging {
  def assert(appSettings: AppSettings): Unit = {

    val consumerProps: Properties = {
      val p = new Properties()
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "AssertSinkTopic")
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      /**
        * Consumer Transaction Isolation Level
        *
        * Required to ensure that the consumer only reads committed messages from the topic.
        */
      p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed")
      p
    }

    logger.info(s"Consumer properties " + consumerProps.toString)

    val consumer = new KConsumer(consumerProps, new StringDeserializer, new StringDeserializer)
    consumer.subscribe(List(appSettings.dataSinkTopic).asJava)

    val stateSeed = SinkState(List[(Int,String,String)](), appSettings.partitions, appSettings.messagesPerPartition)

    Stream.continually(consumer.poll(CONSUMER_POLL_TIMEOUT))
      /**
        * This is something that akka-streams would do well: accumulate state until some condition, then reset state.
        * For our purposes we will just restart the the AssertSink app
        * See an example here:
        * http://doc.akka.io/docs/akka/snapshot/scala/stream/stream-cookbook.html#calculating-the-digest-of-a-bytestring-stream
        */
      .scanLeft(stateSeed) { (sinkState, consumerRecords) =>
        logger.info(s"Sink state begin with ${consumerRecords.count()} records")
        val records = consumerRecords.iterator().asScala.map { record =>
          logger.info(s"Partition ${record.partition()}, Key ${record.key()}, Value: ${record.value()}")
          (record.partition(), record.key(), record.value())
        }
        sinkState.copy(records = sinkState.records ++ records)
      }
      .find(_.assert())
      .foreach { _ =>
        logger.info("Assertion succeeded!")
        consumer.close()
      }
  }

  case class SinkState(records: List[(Int, String, String)], partitions: Int, messagesPerPartition: Int) {

    val controlMessages = 2

    def assert(): Boolean = {
      logger.info(s"Assert begin. Params: partitions: $partitions, messagesPerPartition: $messagesPerPartition, controlMessages: $controlMessages")

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
        logger.info(s"Records length ${records.length}")
        records.length should equal ((messagesPerPartition + controlMessages) * partitions)

        val byPartition = records.groupBy { case (partition, _, _) => partition }

        /**
          * There should be exactly partitionCount number of partitions
          */
        logger.info(s"Partitions ${byPartition.size}")
        byPartition.size should equal (partitions)

        /**
          * Each partition should have exactly messagesPerPartition number of messages
          */
        byPartition.map { case (_, partitionMessages) =>
          logger.info(s"Partition messages ${partitionMessages.length}")
          partitionMessages.length should equal (messagesPerPartition + controlMessages)
        }

        (0 until partitions).map { partition =>
          val numberedMessages = (0 until messagesPerPartition).map { counter =>
            (partition, s"P$partition-$counter", (counter * 2).toString)
          }
          val expectedPartitionRecords = Seq((partition, s"P$partition-START", "START_TRANSFORM")) ++ numberedMessages ++ Seq((partition, s"P$partition-END", "END_TRANSFORM"))

          /**
            * Asserts that the generated expected records occur in order in the actual records.  Because Kafka only
            * guarantees message order within a partition we can only assert order per partition.  ScalaTest has a nice
            * collections matcher to help here.
            *
            * http://doc.scalatest.org/3.0.0/index.html#org.scalatest.Matchers@inOrderElementsOf[R](elements:scala.collection.GenTraversable[R]):org.scalatest.words.ResultOfInOrderElementsOfApplication
            */
          records should contain inOrderElementsOf expectedPartitionRecords
        }
      }.isSuccess
    }
  }
}
