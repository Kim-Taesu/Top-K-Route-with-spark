package smu.datalab.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.{ParamConfig, PathConfig}
import smu.datalab.spark.util.Utils._

import scala.collection.mutable.ListBuffer
import scala.sys.exit

object TopK {

  val log: Logger = LogManager.getRootLogger

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
    val pathConf: PathConfig = PathConfig(conf)

    val saveFileFormat: String = paramConf.value.saveFileFormat
    val originDataDF: DataFrame = loadDataFrame(spark, s"${pathConf.originDataPath}/*.$saveFileFormat")
      .select(ORIGIN_START, ORIGIN_END, PROB)
      .withColumnRenamed(PROB, ORIGIN_PROB)
      .withColumn(ID, row_number.over(Window.partitionBy().orderBy(ORIGIN_START, ORIGIN_END)))
    val noiseDataDF: DataFrame = loadDataFrame(spark, s"${pathConf.noiseDataPath}/*.$saveFileFormat")
      .groupBy(NOISE_START, NOISE_END).count()
    val eStepInitDataDF: DataFrame = loadDataFrame(spark, s"${pathConf.eStepInitDataPath}/*.$saveFileFormat")

    originDataDF.cache()
    eStepInitDataDF.cache()
    noiseDataDF.cache()

    val destCodeSize: Int = noiseDataDF.select(NOISE_START).first().getString(0).length
    val threshold: Double = paramConf.value.threshold
    val iteratorCnt: Int = paramConf.value.iteratorCnt
    val dataTotal: Long = noiseDataDF.select(COUNT).rdd.map(_.getLong(0)).reduce(_ + _)
    var find: Boolean = false
    val preThetaList: ListBuffer[(Double, Int)] = ListBuffer
      .fill(destCodeSize * destCodeSize)(1.0 / (destCodeSize * destCodeSize))
      .zip(originDataDF.select(ID).collect().map(_.getInt(0)))

    log.info(s"[param info] dest code size: $destCodeSize")
    log.info(s"[param info] threshold: $threshold")
    log.info(s"[param info] iterator max: $iteratorCnt")
    log.info(s"[param info] data total size: $dataTotal")

    for (i <- 1 to iteratorCnt if !find) {
      val startTime: Long = System.currentTimeMillis()
      val preThetaDF: DataFrame = originDataDF.join(preThetaList.toDF(PRE_THETA, ID), ID)

      val eStepDataDF: DataFrame = eStepInitDataDF
        .join(broadcast(preThetaDF), Seq(ORIGIN_START, ORIGIN_END))
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

      val theta: Array[Double] = mStepDataDF.select(THETA).collect().map(_.getDouble(0))

      var maxValue: Double = 0.0
      for (i <- theta.indices) {
        val currentTheta: Double = preThetaList(i)._1
        val newTheta: Double = theta(i)
        val thetaDiff: Double = currentTheta - newTheta
        maxValue = Math.max(maxValue, Math.abs(thetaDiff))
      }

      if (maxValue >= threshold) {
        find = true
      }

      copyTheta(preThetaList, theta)
      log.info(s"[em processing] ${i}th: $maxValue (${(System.currentTimeMillis() - startTime) / 1000.0}s)")
    }

    val thetaTotal: Double = preThetaList.map(_._1).sum
    val finalDF = preThetaList
      .map(item => (item._1 / thetaTotal, item._2))
      .toDF(NOISE_PROB, ID)
      .join(originDataDF, ID)
      .select(ORIGIN_START, ORIGIN_END, ORIGIN_PROB, NOISE_PROB)
      .coalesce(1)
    saveDataFrame(finalDF, s"${pathConf.resultDataPath}/$destCodeSize", CSV)
  }

  private def copyTheta(preThetaList: ListBuffer[(Double, Int)], theta: Array[Double]): Unit = {
    preThetaList.clear()
    for (i <- theta.indices) {
      preThetaList.append((theta(i), i + 1))
    }
  }

  private def checkArgs(args: Array[String]): Unit = {
    if (args.length != 1 || args.apply(0).eq("makeEStepInitData")) {
      log.error(usage)
      log.error(s"[args error] Cased By: ${args.mkString(" ")}")
      exit(1)
    }
  }
}
