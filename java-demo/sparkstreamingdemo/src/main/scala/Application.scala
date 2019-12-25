import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010.{ConsumerStrategies, KafkaUtils, LocationStrategies}
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ming
  * @date 2019/12/20 14:43
  */
object Application {
  def main(args: Array[String]): Unit = {
    // offset保存路径
    val checkpointPath = "D:\\checkpoint"

    val conf = new SparkConf()
      .setAppName("ScalaKafkaStream")
      .setMaster("local[*]")
      .set("spark.streaming.backpressure.enabled", "true")

    val sc = new SparkContext(conf)
    sc.setLogLevel("WARN")

    val ssc = new StreamingContext(sc, Seconds(2))
    ssc.checkpoint(checkpointPath)

    val config = sc.getConf.get("spark.ming.consumer.data", "this is my data")
    println("====================")
    println(config)
    println("====================")

    val bootstrapServers = "223.223.200.50:29092,223.223.200.50:39092,223.223.200.50:49092"
    val groupId = "kafka-group"
    val topicName = "ming-s"
    val maxPoll = 500

    val kafkaParams = Map(
      ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG -> bootstrapServers,
      ConsumerConfig.GROUP_ID_CONFIG -> groupId,
      ConsumerConfig.MAX_POLL_RECORDS_CONFIG -> maxPoll.toString,
      ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer],
      ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG -> classOf[StringDeserializer]
    )

    val kafkaTopicDS = KafkaUtils.createDirectStream(ssc, LocationStrategies.PreferConsistent,
      ConsumerStrategies.Subscribe[String, String](Set(topicName), kafkaParams))

        kafkaTopicDS.map(_.value)
          .flatMap(_.split(" "))
          .map(x => (x, 1L))
          .reduceByKey(_ + _)
          .transform(data => {
            val sortData = data.sortBy(_._2, false)
            sortData
          })
          .print()

    //    kafkaTopicDS.map(x => {
    //      val params = x.value().split("\\|")
    //      (1, params(0))
    //    }).transform(data => {
    //      val sortData = data.sortBy(_._2, false)
    //      sortData
    //    }).print()


    ssc.start()
    ssc.awaitTermination()
  }
}
