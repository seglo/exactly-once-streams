package seglo.apps

/**
  * Created by seglo on 13/07/17.
  */
case class AppSettings(
                        partitions: Int = 3,
                        replicas: Int = 1,
                        messagesPerPartition: Int = 10,
                        messageFailureProbability: Double = 0.05,
                        dataSourceTopic: String = "datasource",
                        dataSinkTopic: String = "datasink",
                        bootstrapServers: String = "localhost:9092"
                      )
