package seglo

import java.util.concurrent.TimeUnit

import org.scalatest._

import scala.concurrent.duration._

class KafkaStreamsTests extends FlatSpec with Matchers with BeforeAndAfterEach {
  val settings = AppSettings()
  val timeout = FiniteDuration(1, MINUTES)
  var s: KafkaLocalServer = null

  override def beforeEach(): Unit = {
    s = KafkaLocalServer(true, Some(LOCAL_STATE_DIR))
    s.start()
    s.createTopic(settings.dataSourceTopic, settings.partitions, settings.replicas)
    s.createTopic(settings.dataSinkTopic, settings.partitions, settings.replicas)
  }

  override def afterEach(): Unit = {
    s.stop()
  }

  "Exactly-Once with Kafka Streams" should "successfully produce exact number of records to destination topic" in {
    SourceData.populate(settings)
    val kafkaStreams = ConsumeTransformProduce.run()
    SinkData.assert(settings)

    kafkaStreams.close(10, TimeUnit.SECONDS)
  }

}
