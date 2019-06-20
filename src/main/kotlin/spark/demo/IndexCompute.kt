package spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.Durations

import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.SaveMode
import java.util.Properties
import com.beust.jcommander.Parameter
import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException

fun compute() {
    val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("`IndexCompute`")
            .orCreate
    val path = "/Users/liubo/IdeaProjects/spark-demo-kotlin/Transaction.csv"
    val data = spark.read().option("header", true).csv(path)
    val prop = Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    data.show()
    data.createOrReplaceTempView("orders")

    val index = spark.sql("select cast(sum(credit_amount)/sum(amount) as float) as credit_amount_percentage, cast(count(credit_amount)/count(amount) as float) as credit_pay_num_percentage, cast(sum(credit_amount)/count(credit_amount) as float) as credit_unit_price, cast(sum(amount)/count(amount) as float) as unit_price, count(amount) as day_orders from orders group by meid")
    index.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/lehui", "target", prop)
}

fun main(args: Array<String>) {
    compute()
}
