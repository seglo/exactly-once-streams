package seglo

import java.util.Collections.singleton
import java.util.{Locale, Properties}

import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.clients.consumer.{ConsumerConfig, OffsetAndMetadata}
import org.apache.kafka.clients.producer.ProducerConfig
import org.apache.kafka.common.errors.{OutOfOrderSequenceException, ProducerFencedException}
import org.apache.kafka.common.requests.IsolationLevel
import org.apache.kafka.common.serialization.{StringDeserializer, StringSerializer}
import org.apache.kafka.common.{KafkaException, TopicPartition}
import seglo.apps._

import scala.collection.JavaConverters._
import scala.collection.mutable

object ConsumeTransformProduce extends App with LazyLogging {

  def getUncommittedOffsets(consumer: KConsumer): java.util.Map[TopicPartition, OffsetAndMetadata] = {
    consumer.assignment.asScala.map { topicPartition =>
      (topicPartition, new OffsetAndMetadata(consumer.position(topicPartition)))
    }.toMap.asJava
  }

  def resetToLastCommittedPositions(consumer: KConsumer): Unit = {
    for (topicPartition <- consumer.assignment.asScala) {
      logger.info(s"consumer assignment: ${topicPartition}")
      val offsetAndMetadata = consumer.committed(topicPartition)
      if (offsetAndMetadata != null) {
        logger.info(s"Resetting partition ${topicPartition.partition()} to offset ${offsetAndMetadata.offset()}")
        consumer.seek(topicPartition, offsetAndMetadata.offset)
      } else consumer.seekToBeginning(singleton(topicPartition))
    }
  }

  def run(): Unit = {
    val appSettings = AppSettings()

    val producerProps: Properties = {
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

      /**
        * Transactional ID
        *
        * Used to enforce that this producer completes any running transactions before a producer with the same
        * transactional ID can create a new transaction.
        */
      p.put(ProducerConfig.TRANSACTIONAL_ID_CONFIG, "ConsumeTransformProduceApp")
      p
    }

    val consumerProps: Properties = {
      val p = new Properties()
      p.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
      p.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, new StringDeserializer)
      p.put(ConsumerConfig.GROUP_ID_CONFIG, "datasource-consume")
      p.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
      p.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest")

      /**
        * Consumer Transaction Isolation Level
        *
        * Required to ensure that the consumer only reads committed messages from the topic.
        */
      //p.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, IsolationLevel.READ_COMMITTED.toString.toLowerCase(Locale.ROOT))
      p
    }

    val CONSUMER_POLL_TIMEOUT = 200L

    val producer = new KProducer(producerProps)
    val consumer = new KConsumer(consumerProps, new StringDeserializer, new StringDeserializer)

    consumer.subscribe(List(appSettings.dataSourceTopic).asJava)

    producer.initTransactions()

    Stream.continually(consumer.poll(CONSUMER_POLL_TIMEOUT))
      /**
        * Accumulate number of empty poll results
        */
      .scanLeft((0, None.asInstanceOf[Option[KConsumerRecords]])) { case ((emptyRecordCount, _), newRecords) =>
        if (newRecords == null || newRecords.isEmpty())
          (emptyRecordCount + 1, None)
        else
          (0, Option(newRecords.asInstanceOf[KConsumerRecords]))
      }
      /**
        * Kill the stream after 5 attempts of empty poll results
        */
      .takeWhile { case (emptyRecordCount, _) =>
        logger.info(s"emptyRecordCount: $emptyRecordCount")
        emptyRecordCount < 5
      }
      .foreach {
        case (_, None) =>
        case (_, Some(records)) =>
          try {
            /**
              * Start a new transaction. This will begin the process of batching the consumed records as well
              * as an records produced as a result of processing the input records.
              *
              * We need to check the response to make sure that this producer is able to initiate a new transaction.
              */
            producer.beginTransaction()

            /**
              * Process the input records and send them to the output topic(s).
              */
            val outputRecords = records.iterator().asScala
              .map { cr =>

                val value = if (Seq("START", "END") contains cr.value()) s"${cr.value()}_TRANSFORM"
                else cr.value().toInt * 2

                new KProducerRecord(appSettings.dataSinkTopic, cr.partition(), cr.key(), value.toString)
              }


            for (record <- outputRecords) {
              logger.info(s"Producing record. Partition ${record.partition()}, Key ${record.key()}, Value: ${record.value()}")
              producer.send(record)

              /**
                * Abort the transaction based on probability of message failing to send.
                */
              if (scala.util.Random.nextDouble() <= appSettings.messageFailureProbability) {
                throw new KafkaException("What we have here is a failure to communicate")
              }
            }

            /**
              * To ensure that the consumed and produced messages are batched, we need to commit the offsets through the
              * producer and not the consumer.  If this returns an error, we should abort the transaction.
              */
            try {
              producer.sendOffsetsToTransaction(
                getUncommittedOffsets(consumer),
                consumerProps.getProperty(ConsumerConfig.GROUP_ID_CONFIG)
              )
            } catch {
              case e: Exception =>
                throw new KafkaException("Sending uncommitted offsets to transaction coordinator failed", e)
            }

            /**
              * Now that we have consumed, processed, and produced a batch of messages, let's commit the results.
              * If this does not report success, then the transaction will be rolled back.
              */
            producer.commitTransaction()
          } catch {
            case e@(_: ProducerFencedException | _: OutOfOrderSequenceException) =>
              // We cannot recover from these errors, so just rethrow them and let the process fail
              logger.error(s"A fatal transaction data integrity error was thrown.", e)
              throw e
            case e: KafkaException =>
              logger.error(s"KafkaException caught: ${e.getMessage()}")
              logger.info(s"Aborting transaction.  Reset consumer to last committed positions.")
              producer.abortTransaction()
              resetToLastCommittedPositions(consumer)
          }
      }
  }

  run()
}
