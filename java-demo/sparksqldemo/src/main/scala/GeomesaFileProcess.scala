import org.apache.spark.{SparkConf, SparkContext}

/**
  * @author ming
  * @date 2019/12/10 20:06
  */
object GeomesaFileProcess {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .setAppName("geomesa-files")
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.network.timeout", "300s")
      .setMaster("local[*]")

    val sc = SparkContext.getOrCreate(sparkConf)

    sc.textFile("file:///D://hbase.table")
      .map(x => {
        val params = x.split(" ")
        params
      })
      .filter(x => x.size > 7)
      .map(x => {
        val path = x(13).replace("/hbase/data/default/", "")
        val params = path.split("_")
        if (params.length >= 2) {
          (params(0), params(1))
        } else {
          (params(0), "")
        }
      })
      .filter(x => x._2 != "" && x._1 != "ming")
      .map(x => (s"${x._1},${x._2}", 1))
      .reduceByKey(_ + _)
      .map(x => {
        val params = x._1.split(",")
        "%s,%s".format(params(0), params(1))
      })
      .sortBy(x => x, true)
      .repartition(1)
      .saveAsTextFile("file:///D://result")

    sc.stop()
  }
}
