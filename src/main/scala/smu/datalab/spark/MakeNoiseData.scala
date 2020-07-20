package smu.datalab.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.{ParamConfig, PathConfig}
import smu.datalab.spark.util.Utils.{buildSparkSession, loadRawDataFrame, saveDataFrame}

import scala.sys.exit
import scala.util.Random

object MakeNoiseData {

  val log: Logger = LogManager.getRootLogger

  type OptionMap = Map[String, Int]
  val usage: String =
    """
      | --------------------------------------
      | Usage: makeNoiseData --dest-size num
      |
      | Example: makeNoiseData --dest-size 15
      | Example: makeNoiseData --dest-size 5
      | --------------------------------------
      |""".stripMargin

  val random: Random.type = Random

  def main(args: Array[String]): Unit = {
    val options: OptionMap = checkArgs(args)
    val destNum: Int = options(DEST_SIZE)

    val spark: SparkSession = buildSparkSession("add noise to raw data")

    val conf = ConfigFactory.load(CONFIG_PATH.toString)
    val paramConf: Broadcast[ParamConfig] = spark.sparkContext.broadcast(ParamConfig(conf))
    val pathConf: PathConfig = PathConfig(conf)

    val destCodeList: Seq[String] = paramConf.value.getDestCodeList(destNum)
    val qValue = paramConf.value.qValue
    val pValue = paramConf.value.pValue
    val rawDataPath: String = pathConf.testSamplePath

    val rawDataDF: DataFrame = loadRawDataFrame(spark, rawDataPath, destCodeList)
    rawDataDF.cache()

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
      .withColumn(NOISE_START, addNoiseUdf(col(START)))
      .withColumn(NOISE_END, addNoiseUdf(col(END)))

    saveDataFrame(noiseDataDF, pathConf.noiseDataPath, paramConf.value.saveFileFormat)
  }

  private def checkArgs(args: _root_.scala.Array[_root_.scala.Predef.String]): OptionMap = {
    if (args.length == 0) {
      log.error(s"[args error] Caused By: args size is 0")
      exit(1)
    }
    val argList: List[String] = args.toList
    val options: OptionMap = nextOption(Map(), argList)
    options
  }

  @scala.annotation.tailrec
  private def nextOption(map: OptionMap, list: List[String]): OptionMap = {

    list match {
      case Nil => map
      case "makeNoiseData" :: tail =>
        nextOption(map, tail)
      case "--dest-size" :: value :: Nil =>
        try {
          value.toInt
        } catch {
          case _: NumberFormatException =>
            log.error(usage)
            log.error(s"[args error] Cased By: Number Format Error ($value) ")
            exit(1)
        }
        nextOption(map ++ Map(DEST_SIZE -> value.toInt), Nil)
      case option :: _ =>
        log.error(usage)
        log.error(s"[args error] Cased By: Unknown option ($option) ")
        exit(1)
    }
  }
}
