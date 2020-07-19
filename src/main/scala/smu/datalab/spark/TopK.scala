package smu.datalab.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.ParamConfig
import smu.datalab.spark.util.Utils._

import scala.collection.mutable.ListBuffer
import scala.sys.exit

object TopK {
  val usage: String =
    """
      | --------------------------------------
      | Usage: topK
      |
      | Example: topK
      | --------------------------------------
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    checkArgs(args)

    val spark: SparkSession = buildSparkSession("top k")
    import spark.implicits._

    val conf = ConfigFactory.load(CONFIG_PATH.toString)
    val paramConf: Broadcast[ParamConfig] = spark.sparkContext.broadcast(ParamConfig(conf))

    val originDataDF: DataFrame = loadDataFrame(spark, ORIGIN_DATA_PATH)
      .select(ORIGIN_START, ORIGIN_END)
      .withColumn(ID, monotonically_increasing_id())
    val noiseDataDF: DataFrame = loadDataFrame(spark, NOISE_DATA_PATH)
      .groupBy(NOISE_START, NOISE_END).count()
    val eStepInitDataDF: DataFrame = loadDataFrame(spark, E_STEP_INIT_DATA_PATH)

    originDataDF.cache()
    eStepInitDataDF.cache()
    noiseDataDF.cache()

    val destCodeSize: Int = noiseDataDF.select(NOISE_START).first().getString(0).length
    val threshold: Double = paramConf.value.threshold
    val iteratorCnt: Int = paramConf.value.iteratorCnt
    val dataTotal: Long = noiseDataDF.select(COUNT).rdd.map(_.getLong(0)).reduce(_ + _)
    val preThetaList = ListBuffer.fill(destCodeSize * destCodeSize)(1.0 / (destCodeSize * destCodeSize))
    var flag: Boolean = true

    for (i <- 1 to iteratorCnt if flag) {

      val preThetaDF: DataFrame = originDataDF
        .join(preThetaList.toDF(PRE_THETA).withColumn(ID, monotonically_increasing_id), ID)

      val eStepDataDF: DataFrame = eStepInitDataDF
        .join(preThetaDF, Seq(ORIGIN_START, ORIGIN_END))
        .withColumn(E_STEP_PROB, col(PRE_THETA) * col(PROB))
        .withColumn(E_STEP_SUM, sum(E_STEP_PROB).over(Window.partitionBy(NOISE_START, NOISE_END)))
        .withColumn(E_STEP_RESULT, col(E_STEP_PROB) / col(E_STEP_SUM))
        .select(NOISE_START, NOISE_END, ORIGIN_START, ORIGIN_END, E_STEP_RESULT)

      val mStepDataDF: DataFrame = noiseDataDF
        .join(eStepDataDF, Seq(NOISE_START, NOISE_END))
        .withColumn(M_STEP_TMP, col(E_STEP_RESULT) * col(COUNT))
        .groupBy(ORIGIN_START, ORIGIN_END).agg(sum(M_STEP_TMP).as(E_STEP_RESULT_SUM))
        .select(col(ORIGIN_START), col(ORIGIN_END), (col(E_STEP_RESULT_SUM) / dataTotal).as(THETA))
        .orderBy(ORIGIN_START, ORIGIN_END)

      val theta: Array[Double] = mStepDataDF.collect().map(_.getDouble(2))

      var maxValue: Double = 0.0
      for (i <- preThetaList.indices) {
        val pre_value: Double = preThetaList(i)
        val new_value: Double = theta(i)
        maxValue = Math.max(maxValue, Math.abs(pre_value - new_value))
      }

      if (maxValue >= threshold) {
        flag = false
      }

      // success
      if (flag) {
        preThetaList.clear()
        theta.foreach(item => {
          preThetaList.append(item)
        })
      }
      else {
        println("iterator count\t" + i)
        println("max value\t" + maxValue)
        val thetaTotal: Double = theta.sum
        theta.foreach(item => {
          println(item / thetaTotal)
        })
      }
    }
  }

  private def checkArgs(args: Array[String]): Unit = {
    if (args.length != 1 || args.apply(0).eq("makeEStepInitData")) {
      println(usage)
      println(s" Error: ${args.mkString(" ")}")
      exit(1)
    }
  }
}
