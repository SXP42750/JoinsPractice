import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions._
import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{SaveMode, SparkSession}
import org.apache.spark.sql.expressions.Window

object EmployeeJoin {

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

    val employees = Seq(
      ("E001", "Alice", "D001"),
      ("E002", "Bob", "D002"),
      ("E003", "Charlie", "D001"),
      ("E004", "David", "D003"),
      ("E005", "Eve", "D002"),
      ("E006", "Frank", "D001"),
      ("E007", "Grace", "D004")

    ).toDF("employee_id", "employee_name", "department_id")

    val departments = Seq(
      ("D001", "HR"),
    ("D002", "Finance"),
    ("D003", "IT"),
    ("D004", "Marketing"),
    ("D005", "Sales")
    ).toDF("department_id", "department_name")

    val condition = employees.col("department_id")=== departments.col("department_id")
    val joinType = "Inner"
    val joined  = employees.join(departments,condition,joinType).drop(departments("department_id"))
      joined.show()

  }
}

