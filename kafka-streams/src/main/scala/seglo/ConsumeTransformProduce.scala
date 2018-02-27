package seglo

import java.lang.Long
import java.util.Properties
import java.util.concurrent.TimeUnit

import kafka.producer.ProducerConfig
import org.apache.kafka.common.KafkaException
import org.apache.kafka.common.serialization._
import org.apache.kafka.common.utils.Bytes
import org.apache.kafka.streams._
import org.apache.kafka.streams.kstream._
import org.apache.kafka.streams.state.KeyValueStore
import com.typesafe.scalalogging.LazyLogging
import org.apache.kafka.streams.processor.StreamPartitioner

import scala.collection.JavaConverters.asJavaIterableConverter

/**
  * Kafka Streams processing guarantees:
  * https://kafka.apache.org/10/documentation/streams/core-concepts#streams_processing_guarantee
  */
object ConsumeTransformProduce extends App with LazyLogging {
  def run(): KafkaStreams = {
    val appSettings = AppSettings()

    /**
      * Kafka Streams properties: https://kafka.apache.org/10/documentation/#streamsconfigs
      */
    val config: Properties = {
      val p = new Properties()
      p.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams-exactly-once")
      p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appSettings.bootstrapServers)
      p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      p.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass)
      /**
        * Enable exactly once processing guarantees in consume->transform->produce workflow, including statestores that
        * may be used.
        */
      p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, "exactly_once")
      p
    }

    val builder: StreamsBuilder = new StreamsBuilder()
    val dataSource: KStream[String, String] = builder
      .stream(appSettings.dataSourceTopic)
      /**
        * Process the input records and send them to the output topic(s).
        */
      .map((key: String, value: String) => {
        /**
          * Abort the transaction based on probability of message failing to send.
          *
          * Note: There's no easy way to simulate a transient failure that will trigger kafka streams to restart.  The
          * EoS (exactly-once) KIP for Kafka Streams says only certain types of transient failures such as when a broker
          * temporarily loses connection will trigger retries and consumer rollbacks.  Supporting user-defined
          * exceptions the same way is discussed in the KIP, but not currently implemented. So you'll just have to take
          * my word for it that this works :)
          *
          * https://cwiki.apache.org/confluence/display/KAFKA/KIP-129%3A+Streams+Exactly-Once+Semantics
          */
//        if (scala.util.Random.nextDouble() <= appSettings.messageFailureProbability) {
//          logger.error(s"Aborting transaction.  Throwing RuntimeException.")
//          throw new RuntimeException("What we have here is a failure to communicate")
//        }

        val newValue: String =
          if (Seq("START", "END") contains value) s"${value}_TRANSFORM"
          else (Integer.parseInt(value) * 2).toString

        logger.info(s"Producing record. Key $key, Value: $value")
        new KeyValue[String, String](key, newValue)
      })

    dataSource.to(appSettings.dataSinkTopic,
      Produced.
        `with`(Serdes.String(), Serdes.String())
        /**
          * Guarantee that messages from data-source topic go into same partition as data-sink topic.
          */
        .withStreamPartitioner(new StreamPartitioner[String, String] {
          override def partition(key: String, value: String, numPartitions: Int): Integer = {
            """P(\d+)\-.*""".r.findFirstMatchIn(key).map(m => m.group(1).toInt).getOrElse {
              throw new KafkaException("This should never happen.")
            }
          }
        }))


    val streams: KafkaStreams = new KafkaStreams(builder.build(), config)
    streams.start()

    streams
  }

  run()
}
