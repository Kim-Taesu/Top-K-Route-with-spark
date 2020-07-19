package smu.datalab.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.Logger
import org.apache.log4j.lf5.LogLevel._
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.{PathConfig, _}
import smu.datalab.spark.util.Utils._

import scala.util.Random

object AddNoise {
  val random: Random.type = Random
  val logger: Logger = Logger.getLogger(getClass.getName)

  def main(args: Array[String]): Unit = {

    val dataSize: String = "some"
    val destNum: Int = 15
    logger.info("dataSize: " + dataSize)
    logger.debug("destNum: " + destNum)

    val spark: SparkSession = buildSparkSession("add noise to raw data")
    setLogLevel(spark, WARN)
    import spark.implicits._

    val conf = ConfigFactory.load(CONFIG_PATH.toString)

    val paramConf: Broadcast[ParamConfig] = spark.sparkContext.broadcast(ParamConfig(conf))
    val destCodeList: Seq[String] = paramConf.value.getDestCodeList(destNum)
    val qValue = paramConf.value.qValue
    val pValue = paramConf.value.pValue

    val pathConf: PathConfig = PathConfig(conf)
    val rawDataPath: String = pathConf.testSamplePath

    val getBitMask: String => String = dest => {
      val index = destCodeList.indexOf(dest)
      val prefix = NO_EXIST * index
      val postfix = NO_EXIST * (destCodeList.size - (index + 1))
      prefix + EXIST + postfix
    }

    val makeBitMaskUdf: UserDefinedFunction = udf(getBitMask(_: String))

    val rawDataDF: DataFrame = loadRawDataFrame(spark, rawDataPath, destCodeList)
    rawDataDF.cache()

    val noiseStart: String = makeColName(NOISE, START)
    val noiseEnd: String = makeColName(NOISE, END)
    val originStart: String = makeColName(ORIGIN, START)
    val originEnd: String = makeColName(ORIGIN, END)

    val originDataCountDF: DataFrame = rawDataDF
      .withColumn(originStart, makeBitMaskUdf(col(START)))
      .withColumn(originEnd, makeBitMaskUdf(col(END)))
      .groupBy(originStart, originEnd).count()

    val total: Broadcast[Long] = spark.sparkContext.broadcast(originDataCountDF.agg(sum(COUNT)).collect().map(_.getLong(0)).apply(0))

    val originDataProbDF: DataFrame = originDataCountDF
      .select(col(originStart), col(originEnd), col(COUNT) / lit(total.value))


    //  노이즈 추가된 a, b 위치 정보
    val addNoise: String => String = dest => {
      val index = destCodeList.indexOf(dest)
      var result = EMPTY_STRING
      for (i <- destCodeList.indices) {
        val prob = random.nextDouble()
        if (i == index) {
          val nextItem = if (prob < pValue) EXIST else NO_EXIST
          result = result.concat(nextItem)
        } else {
          val nextItem = if (prob < qValue) EXIST else NO_EXIST
          result = result.concat(nextItem)
        }
      }
      result
    }
    val addNoiseUdf: UserDefinedFunction = udf(addNoise(_: String))

    val noiseDataDF: DataFrame = rawDataDF
      .withColumn(noiseStart, addNoiseUdf(col(START)))
      .withColumn(noiseEnd, addNoiseUdf(col(END)))


    // a -> b 이동 경우의 수 (원본)
    val originArray: Seq[String] = for (i <- destCodeList.indices) yield NO_EXIST * destCodeList.length + (1 << i).toBinaryString takeRight destCodeList.length
    val originCase: DataFrame = originArray.toList.toDF(originStart)
    val originCaseDF: Dataset[Row] = originCase
      .crossJoin(originCase.withColumnRenamed(originStart, originEnd))

    // 특정 노이즈가 어떤 지역구 비트마스크에서 왔는지 확률
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
    val probNoiseStartToOriginStart: String = makeColName(noiseStart, originStart, PROB)
    val probNoiseEndToOriginEnd: String = makeColName(noiseEnd, originEnd, PROB)
    val eStepInitDataDF: DataFrame = noiseDataDF
      .distinct()
      .crossJoin(originCaseDF)
      .withColumn(probNoiseStartToOriginStart, getProbUdf(col(noiseStart), col(originStart)))
      .withColumn(probNoiseEndToOriginEnd, getProbUdf(col(noiseEnd), col(originEnd)))
      .withColumn(PROB, col(probNoiseStartToOriginStart) * col(probNoiseEndToOriginEnd))
      .select(noiseStart, originStart, noiseEnd, originEnd, PROB)

    eStepInitDataDF.show()
  }

}
