package spark.demo

import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.Durations
import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.SaveMode

import java.util.Properties

fun compute() {
    val spark = SparkSession
            .builder()
            .master("local[*]")
            .appName("`IndexCompute`")
            .orCreate

    //模拟Transaction表
    val path = "/Users/liubo/IdeaProjects/spark-demo-kotlin/Transaction.csv"
    val data = spark.read().option("header", true).csv(path)

    //mysql配置
    val prop = Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "root")

    //data.show()
    //建立临时表
    data.createOrReplaceTempView("target")

    //计算信用卡金额占比，信用卡支付比例，信用客单价，总客单价，每日交易笔数
    val index = spark.sql("select cast(sum(credit_amount)/sum(amount) as float) as credit_amount_percentage, cast(count(credit_amount)/count(amount) as float) as credit_pay_num_percentage, cast(sum(credit_amount)/count(credit_amount) as float) as credit_unit_price, cast(sum(amount)/count(amount) as float) as unit_price, count(amount) as day_orders from target group by meid")
    index.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/lehui", "target", prop)
}

fun main(args: Array<String>) {
    //开始计算每日指标
    compute()
}
