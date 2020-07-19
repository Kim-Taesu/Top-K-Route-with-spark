package smu.datalab.spark

import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.{ParamConfig, PathConfig}
import smu.datalab.spark.util.Utils.{buildSparkSession, loadDataFrame, makeColName, saveDataFrame}

import scala.sys.exit
import scala.util.Random

object MakeEStepInitData {
  val usage: String =
    """
      | --------------------------------------
      | Usage: makeEStepInitData
      |
      | Example: makeEStepInitData
      | --------------------------------------
      |""".stripMargin
  val random: Random.type = Random

  def main(args: Array[String]): Unit = {

    checkArgs(args)

    val spark: SparkSession = buildSparkSession("make e step init data")
    import spark.implicits._

    val conf = ConfigFactory.load(CONFIG_PATH.toString)
    val paramConf: Broadcast[ParamConfig] = spark.sparkContext.broadcast(ParamConfig(conf))
    val pathConf: PathConfig = PathConfig(conf)

    val qValue = paramConf.value.qValue
    val pValue = paramConf.value.pValue
    val saveFileFormat = paramConf.value.saveFileFormat
    val probNoiseStartToOriginStart: String = makeColName(NOISE_START, ORIGIN_START, PROB)
    val probNoiseEndToOriginEnd: String = makeColName(NOISE_END, ORIGIN_END, PROB)

    val noiseDataDF: DataFrame = loadDataFrame(spark, s"${pathConf.noiseDataPath}/*.$saveFileFormat")
    val noiseBitMaskRowTmp: String = noiseDataDF.select(NOISE_START).first().getString(0)

    val originArray: Seq[String] = for (i <- noiseBitMaskRowTmp.indices) yield NO_EXIST * noiseBitMaskRowTmp.length + (1 << i).toBinaryString takeRight noiseBitMaskRowTmp.length
    val originCaseDF: DataFrame = originArray.toList.toDF(ORIGIN_START)
    val originCaseCrossedDF: DataFrame = originCaseDF
      .crossJoin(originCaseDF.withColumnRenamed(ORIGIN_START, ORIGIN_END))

    val getProb: (String, String) => Double = (noise, origin) => {
      var prob: Double = 1.0
      for (i <- 0 until origin.length) {
        if (origin.charAt(i) == noise.charAt(i)) {
          val nextProb = if (origin.charAt(i).toString == EXIST) 1 - pValue else 1 - qValue
          prob = prob * nextProb
        }
        else {
          val nextProb = if (origin.charAt(i).toString == EXIST) pValue else qValue
          prob = prob * nextProb
        }
      }
      prob
    }
    val getProbUdf: UserDefinedFunction = udf(getProb(_: String, _: String))

    val eStepInitDataDF: DataFrame = noiseDataDF
      .distinct()
      .crossJoin(originCaseCrossedDF)
      .withColumn(probNoiseStartToOriginStart, getProbUdf(col(NOISE_START), col(ORIGIN_START)))
      .withColumn(probNoiseEndToOriginEnd, getProbUdf(col(NOISE_END), col(ORIGIN_END)))
      .withColumn(PROB, col(probNoiseStartToOriginStart) * col(probNoiseEndToOriginEnd))
      .select(NOISE_START, ORIGIN_START, NOISE_END, ORIGIN_END, PROB)

    saveDataFrame(eStepInitDataDF, pathConf.eStepInitDataPath, saveFileFormat)
  }

  private def checkArgs(args: Array[String]): Unit = {
    if (args.length != 1 || args.apply(0).eq("makeEStepInitData")) {
      println(usage)
      println(s" Error: ${args.mkString(" ")}")
      exit(1)
    }
  }

}
