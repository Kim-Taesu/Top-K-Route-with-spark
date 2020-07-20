package smu.datalab.spark

import com.typesafe.config.ConfigFactory
import org.apache.log4j.{LogManager, Logger}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.{ParamConfig, PathConfig}
import smu.datalab.spark.util.Utils.{buildSparkSession, loadRawDataFrame, saveDataFrame}

import scala.sys.exit

object MakeOriginData {

  val log: Logger = LogManager.getRootLogger

  type OptionMap = Map[String, Int]
  val usage: String =
    """
      | --------------------------------------
      | Usage: makeOriginData --dest-size num
      |
      | Example: makeOriginData --dest-size 15
      | Example: makeOriginData --dest-size 5
      | --------------------------------------
      |""".stripMargin

  def main(args: Array[String]): Unit = {

    val options: OptionMap = checkArgs(args)
    val destNum: Int = options(DEST_SIZE)

    val spark: SparkSession = buildSparkSession("make origin data prob")

    val conf = ConfigFactory.load(CONFIG_PATH.toString)
    val paramConf: ParamConfig = ParamConfig(conf)
    val pathConf: PathConfig = PathConfig(conf)

    val destCodeList: Seq[String] = paramConf.getDestCodeList(destNum)
    val rawDataPath: String = pathConf.testSamplePath

    val rawDataDF: DataFrame = loadRawDataFrame(spark, rawDataPath, destCodeList)
    rawDataDF.cache()

    val getBitMask: String => String = dest => {
      val index = destCodeList.indexOf(dest)
      val prefix = NO_EXIST * index
      val postfix = NO_EXIST * (destCodeList.size - (index + 1))
      prefix + EXIST + postfix
    }
    val makeBitMaskUdf: UserDefinedFunction = udf(getBitMask(_: String))

    val originDataCountDF: DataFrame = rawDataDF
      .withColumn(ORIGIN_START, makeBitMaskUdf(col(START)))
      .withColumn(ORIGIN_END, makeBitMaskUdf(col(END)))
      .groupBy(ORIGIN_START, ORIGIN_END).count()

    val total: Broadcast[Long] = spark.sparkContext.broadcast(originDataCountDF.agg(sum(COUNT)).collect().map(_.getLong(0)).apply(0))

    val originDataProbDF: DataFrame = originDataCountDF
      .withColumn(PROB, col(COUNT) / lit(total.value))
      .select(ORIGIN_START, ORIGIN_END, PROB)

    saveDataFrame(originDataProbDF, pathConf.originDataPath, paramConf.saveFileFormat)
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
      case "makeOriginData" :: tail =>
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
