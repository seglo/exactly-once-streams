package seglo

import org.scalatest._
import scala.concurrent.duration._

class KafkaClientTests extends FlatSpec with Matchers with BeforeAndAfterEach {
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

  "Exactly-Once with Kafka Clients" should "successfully produce exact number of records to destination topic" in {
    SourceData.populate(settings)
    ConsumeTransformProduce.run()
    SinkData.assert(settings)
  }

}
