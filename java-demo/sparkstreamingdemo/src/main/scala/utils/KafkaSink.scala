package utils

import java.util.concurrent.Future
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerRecord, RecordMetadata}

/**
  * @author ming
  * @date 2019/12/20 9:52
  */

class KafkaSink[K, V](createProducer: () => KafkaProducer[K, V]) extends Serializable {
  /* This is the key idea that allows us to work around running into
     NotSerializableExceptions. */
  lazy val producer = createProducer()

  def send(topic: String, key: K, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, key, value))
  }

  def send(topic: String, value: V): Future[RecordMetadata] = {
    producer.send(new ProducerRecord[K, V](topic, value))
  }
}

object KafkaSink {

  import scala.collection.JavaConversions._

  def apply[K, V](config: Map[String, Object]): KafkaSink[K, V] = {
    val createProducerFunc = () => {
      val producer = new KafkaProducer[K, V](config)
      sys.addShutdownHook {
        // Ensure that, on executor JVM shutdown, the Kafka producer sends
        // any buffered messages to Kafka before shutting down.
        producer.close()
      }
      producer
    }
    new KafkaSink(createProducerFunc)
  }

  def apply[K, V](config: java.util.Properties): KafkaSink[K, V] = apply(config.toMap)
}


//// 初始化KafkaSink,并广播
//val kafkaProducer: Broadcast[KafkaSink[String, String]] = {
//  val kafkaProducerConfig = {
//  val p = new Properties()
//  p.setProperty("bootstrap.servers", bootstrapServers)
//  p.setProperty("key.serializer", classOf[StringSerializer].getName)
//  p.setProperty("value.serializer", classOf[StringSerializer].getName)
//  p
//}
//  if (LOG.isInfoEnabled)
//  LOG.info("kafka producer init done!")
//  ssc.sparkContext.broadcast(KafkaSink[String, String](kafkaProducerConfig))
//}

