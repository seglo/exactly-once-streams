package seglo.impl

import akka.Done
import akka.stream.scaladsl.{Flow, Keep, Sink}
import org.apache.kafka.clients.producer.RecordMetadata

import scala.concurrent.{Future, Promise}

object Producer {
  def plainSink(producer: KProducer): Sink[KProducerRecord, Future[Done]] = {

    Flow[KProducerRecord].mapAsync(1) { message =>
      println(s"Producing: $message")

      val r = Promise[KResult[KProducerRecord]]
      producer.send(message, (metadata: RecordMetadata, exception: Exception) => {
        if (exception == null) {
          println(s"Successfully produced: $message")
          r.success(KResult(metadata, message))
        } else {
          println(s"Failed to produce: $message")
          println(exception)
          r.failure(exception)
        }
      })

      r.future
    }
    .toMat(Sink.ignore)(Keep.right)
  }
}
