package smu.datalab.spark.util

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{Column, DataFrame, SaveMode, SparkSession}
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.schema.Schema.rawDataSchema

object Utils {

  def makePath(paths: String*): String = {
    paths.mkString(".")
  }

  def buildSparkSession(appName: String): SparkSession = {
    SparkSession.builder()
      .master("local[*]")
      .appName(appName)
      .config("spark.sql.shuffle.partitions", 16)
      .config("spark.sql.crossJoin.enabled", value = true)
      .config("org.apache.spark.serializer.KryoSerializer", value = true)
      .getOrCreate()
  }

  def makeColList(cols: String*): Seq[Column] = {
    cols.map(col)
  }

  def makeColName(cols: String*): String = {
    cols.mkString("_")
  }

  def loadRawDataFrame(spark: SparkSession, rawDataPath: String, destCodeList: Seq[String]): DataFrame = {
    spark.read
      .option(HEADER, value = true)
      .schema(rawDataSchema)
      .csv(rawDataPath)
      .select(TAXI_DATA_FRAME_COLS.head, TAXI_DATA_FRAME_COLS.tail: _*)
      .filter(row => destCodeList.contains(row.getString(RAW_DATA_DEST_COL_INDEX)))
      .withColumnRenamed(DEST, START)
      .withColumn(END, lead(START, 1)
        .over(Window.partitionBy(TAXI_ID).orderBy(DAY, TIME)))
      .na.drop()
  }

  def loadDataFrame(sparkSession: SparkSession, path: String): DataFrame = {
    sparkSession.read.load(path)
  }

  def saveDataFrame(dataFrame: DataFrame, savePath: String, saveFileFormat: String): Unit = {
    dataFrame.write
      .mode(SaveMode.Overwrite)
      .format(saveFileFormat)
      .save(savePath)
  }

}
