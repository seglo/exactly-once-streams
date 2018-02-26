package seglo

import akka.Done
import org.scalatest._
import seglo.apps.{AppSettings, ConsumeTransformProduce}
import seglo.server.KafkaLocalServer
import util._

import scala.concurrent.{Await, duration}
import scala.concurrent.duration._

class KafkaClientTests extends FlatSpec with Matchers with BeforeAndAfterEach {
  val settings = AppSettings()
  val timeout = FiniteDuration(1, MINUTES)
  var s: KafkaLocalServer = null

  override def beforeEach(): Unit = {
    s = KafkaLocalServer(true, Some(localStateDir))
    s.start()
    s.createTopic(settings.dataSourceTopic, settings.partitions, settings.replicas)
    s.createTopic(settings.dataSinkTopic, settings.partitions, settings.replicas)
  }

  override def afterEach(): Unit = {
    s.stop()
  }

  "Exactly-Once with Kafka Clients" should "successfully produce exact number of records to destination topic" in {

    Await.result(SourceData.populate(settings), timeout)

    ConsumeTransformProduce.run()

    SinkData.assert(settings)
  }

}
