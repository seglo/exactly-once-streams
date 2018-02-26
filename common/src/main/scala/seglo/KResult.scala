package seglo

import org.apache.kafka.clients.producer.RecordMetadata

case class KResult[M](metadata: RecordMetadata, msg: M)
