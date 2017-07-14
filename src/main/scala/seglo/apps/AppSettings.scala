package seglo.apps

/**
  * Created by seglo on 13/07/17.
  */
case class AppSettings(
                        partitionCount: Int = 3,
                        messagesPerPartition: Int = 10,
                        transactionFailureProbability: Double = 0.01,
                        dataSourceTopic: String = "datasource",
                        dataSinkTopic: String = "datasink",
                        bootstrapServers: String = "localhost:9092"
                      )
