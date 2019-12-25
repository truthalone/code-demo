import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

/**
  * @author ming
  * @date 2019/12/9 9:29
  */
object SqlDemo1 {
  def main(args: Array[String]): Unit = {
    val sparkConf = new SparkConf()
      .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      .set("spark.network.timeout", "300s")
      .setMaster("local[*]")

    val ss = SparkSession.builder().config(sparkConf).appName("sql-demo").getOrCreate()

    val df = ss.read.json("hdfs://host51.ehl.com:9000/user/ming/sql/employee.json")
//    df.show()
    df.createOrReplaceTempView("employee")

    df.sqlContext.sql("SELECT * FROM  employee where salary >= 7000 ORDER BY salary desc")
        .show()

    ss.stop()
  }
}
