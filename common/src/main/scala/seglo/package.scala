package seglo

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord}

package object apps {
  type K = String
  type V = String
  type KProducerRecord = ProducerRecord[K, V]
  type KConsumerRecords = ConsumerRecords[K, V]
  type KProducer = KafkaProducer[K, V]
  type KConsumer = KafkaConsumer[K, V]
}
