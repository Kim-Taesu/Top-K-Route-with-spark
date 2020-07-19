package smu.datalab.spark.init

import com.typesafe.config.ConfigFactory
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, sum, udf}
import org.apache.spark.sql.{DataFrame, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.config.{ParamConfig, PathConfig}
import smu.datalab.spark.util.Utils.{buildSparkSession, loadRawDataFrame, makeColName, saveDataFrame}

import scala.sys.exit

object MakeOriginData {
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

    val paramConf: Broadcast[ParamConfig] = spark.sparkContext.broadcast(ParamConfig(conf))
    val destCodeList: Seq[String] = paramConf.value.getDestCodeList(destNum)

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

    val originStart: String = makeColName(ORIGIN, START)
    val originEnd: String = makeColName(ORIGIN, END)

    val originDataCountDF: DataFrame = rawDataDF
      .withColumn(originStart, makeBitMaskUdf(col(START)))
      .withColumn(originEnd, makeBitMaskUdf(col(END)))
      .groupBy(originStart, originEnd).count()

    val total: Broadcast[Long] = spark.sparkContext.broadcast(originDataCountDF.agg(sum(COUNT)).collect().map(_.getLong(0)).apply(0))

    val originDataProbDF: DataFrame = originDataCountDF
      .withColumn(PROB, col(COUNT) / lit(total.value))
      .select(originStart, originEnd, PROB)

    saveDataFrame(originDataProbDF, pathConf.originDataProbPath, paramConf.value.saveFileFormat)
  }


  private def checkArgs(args: _root_.scala.Array[_root_.scala.Predef.String]): OptionMap = {
    if (args.length == 0) {
      println(usage)
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
            println(usage)
            println(" Number Format Error: " + value)
            exit(1)
        }
        nextOption(map ++ Map(DEST_SIZE -> value.toInt), Nil)
      case option :: _ =>
        println(usage)
        println(" Unknown option: " + option)
        exit(1)
    }
  }
}
