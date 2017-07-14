package seglo.apps

/**
  * Created by seglo on 13/07/17.
  */
case class AppSettings(
                        dataSourceTopic: String = "datasource",
                        dataSinkTopic: String = "datasink",
                        bootstrapServers: String = "localhost:9092"
                      )
