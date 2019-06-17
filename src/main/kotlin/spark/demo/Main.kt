package spark.demo


import org.apache.spark.SparkConf
import org.apache.spark.api.java.JavaSparkContext
import org.apache.spark.streaming.Durations

import org.apache.spark.streaming.api.java.JavaStreamingContext
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.Row
import org.apache.spark.sql.SaveMode
import java.util.regex.Pattern
import java.util.Properties
import  java.sql.SQLException
import com.beust.jcommander.Parameter
import com.beust.jcommander.JCommander
import com.beust.jcommander.ParameterException
import  java.net.ConnectException


private fun streamingdemo(hostname: String, port: Int) {
    val SPACE = Pattern.compile(" ")
    val sc = JavaSparkContext("local[*]", "Kotlin Spark Streaming Test")
    val ssc = JavaStreamingContext(sc, Durations.seconds(1))
    val lines = ssc.socketTextStream(hostname, port, StorageLevels.MEMORY_AND_DISK_SER)
    val words = lines.flatMap { l: String -> SPACE.split(l).iterator() }
    sc.setLogLevel("ERROR")
    words.foreachRDD { rdd, _ ->
        run {

            try {
                val spark = JavaSparkSessionSingleton.getInstance(rdd.context().conf)

                val rowRDD = rdd.map { word ->
                    val record = JavaRecord()
                    if (word != "") {
                        record.word = word
                    } else {
                        record.word = "empty"
                    }

                    record
                }

                val df = spark!!.createDataFrame(rowRDD, JavaRecord().javaClass)
                if (df.count() != 0L) {
                    df.createOrReplaceTempView("words")
                    val result = spark.sql("select word, count(*) as total from words group by word")

                    result.show()
                }
            } catch (e: IllegalArgumentException) {
                println(e.message)
            }
        }
    }

    ssc.start()
    ssc.awaitTermination()
}

fun wordcount() {
    val conf = SparkConf()
        .setMaster("local")
        .setAppName("Kotlin Spark Test")

    val sc = JavaSparkContext(conf)

    val items = listOf("123/643/7563/2134/ALPHA", "2343/6356/BETA/2342/12", "23423/656/343")

    val input = sc.parallelize(items)

    val sumOfNumbers = input.flatMap { it.split("/").listIterator() }
        .filter { it.matches(Regex("[0-9]+")) }
        .map { it.toInt() }
        .reduce { total: Int, next: Int -> total + next }

    println(sumOfNumbers)
}

internal object JavaSparkSessionSingleton {
    private var session: SparkSession? = null
    fun getInstance(sparkConf: SparkConf): SparkSession? {
        if (session == null) {
            session = SparkSession
                .builder()
                .config(sparkConf)
                .orCreate
        }

        return session
    }
}

class JavaRecord : java.io.Serializable {
    var word: String = ""
}

fun csvc() {
    val spark = SparkSession
        .builder()
        .master("local[*]")
        .appName("JavaWordCount")
        .orCreate
    val path = "/Users/ken/Desktop/spark-test/df_test.csv"
    val ds = spark.read().option("header", true).csv(path)
    // ds.show()
    ds.createOrReplaceTempView("orders")
    // Dataset统计指标：mean均值，variance方差，stddev标准差，skewness偏度，kurtosis峰度
    val result =
        spark.sql("select merchant_id, mean(amount) as avg_amount,variance(amount) as variance_amount,stddev(amount) as stddev_amount,skewness(amount) as skewness_amount,kurtosis(amount) as kurtosis_amount from orders where group by merchant_id")
    val resultDF = result.filter { value: Row ->
        run {
            !(value.getDouble(1).isNaN() ||
                    value.getDouble(2).isNaN() ||
                    value.getDouble(3).isNaN() ||
                    value.getDouble(4).isNaN() ||
                    value.getDouble(5).isNaN())
        }
    }

    resultDF.show()
    // write to dir
    //result.write().csv("/Users/ken/Desktop/spark-test/result.csv")
    val prop = Properties()
    prop.setProperty("user", "root")
    prop.setProperty("password", "123456")
    try {
        // write to mysql, 表不存在会自动创建
        resultDF.write().mode(SaveMode.Append).jdbc("jdbc:mysql://localhost:3306/spark_test", "orders", prop)
    } catch (e: SQLException) {
        println(e.message)
    }

    spark.stop()
}

class Args {
    @Parameter(names = ["-p", "--port"], description = "nc port")
    var port = 9999

    @Parameter(names = ["-h", "--hostname"], description = "nc hostname")
    var hostname: String = ""

}

fun main(args: Array<String>) {
    val argv = Args()
    val cmd = JCommander.newBuilder()
        .addObject(argv)
        .build()

    try {
        cmd.parse(*args)
    } catch (e: ParameterException) {
        cmd.usage()
        Runtime.getRuntime().exit(1)
    }

    streamingdemo(argv.hostname, argv.port)
}