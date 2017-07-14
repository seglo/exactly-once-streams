package seglo

import org.apache.kafka.clients.consumer.{ConsumerRecords, KafkaConsumer}
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

package object impl {
  type K = String
  type V = String
  type KProducerRecord = ProducerRecord[K, V]
  type KConsumerRecords = ConsumerRecords[K, V]
  type KProducer = KafkaProducer[K, V]
  type KConsumer = KafkaConsumer[K, V]

  case class KResult[M](metadata: RecordMetadata, msg: M)
}
