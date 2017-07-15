package seglo.apps

/**
  * Created by seglo on 13/07/17.
  */
case class AppSettings(
                        partitionCount: Int = 3,
                        messagesPerPartition: Int = 10,
                        messageFailureProbability: Double = 0.10,
                        dataSourceTopic: String = "datasource",
                        dataSinkTopic: String = "datasink",
                        bootstrapServers: String = "localhost:9092"
                      )
