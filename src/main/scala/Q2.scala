import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

object Q2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger(("org")).setLevel(Level.OFF)
    Logger.getLogger(("akka")).setLevel(Level.OFF)

    val sparkconf = new SparkConf()
    sparkconf.set("spark.app.Name", "karthik")
    sparkconf.set("spark.master", "local[*]")
    sparkconf.set("spark.executor.memory", "2g")

    val spark = SparkSession.builder()
      .config(sparkconf)
      .getOrCreate()

    import spark.implicits._

    val products = Seq(
      ("P001", "Laptop"),
      ("P002", "Mobile"),
      ("P003", "Tablet"),
      ("P004", "Monitor"),
      ("P005", "Keyboard")
    ).toDF("product_id", "product_name")

    val sales = Seq(
      ("S001", "P001", 500.0),
      ("S002", "P002", 300.0),
      ("S003", "P001", 700.0),
      ("S004", "P003", 200.0),
      ("S005", "P002", 400.0),
      ("S006", "P004", 600.0),
      ("S007", "P005", 150.0)
    ).toDF("sale_id", "product_id", "amount")

    val condition = products.col("product_id")=== sales.col("product_id")
    val joinType = "Inner"
    val joined  = products.join(sales,condition,joinType).drop(sales("product_id"))
    val group = joined.groupBy(col("product_id"))
      .agg(
        sum(col("amount")).as("Total_amount")
      )
    group.show()

  }
}