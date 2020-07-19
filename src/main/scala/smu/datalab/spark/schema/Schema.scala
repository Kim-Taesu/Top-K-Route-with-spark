package smu.datalab.spark.schema

import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}
import smu.datalab.spark.config.ConfigEnums._

object Schema {
  private val _rawDataSchema = StructType(Seq(
    StructField(TAXI_ID, StringType),
    StructField(DAY, IntegerType),
    StructField(TIME, IntegerType),
    StructField(WEATHER, StringType),
    StructField(DEST, StringType),
    StructField(CNT_ON, StringType),
    StructField(CNT_OFF, StringType),
    StructField(CNT_EMP, StringType),
    StructField(EMPTY, StringType)
  ))

  private val _eStepInitSchema = StructType(Seq(
    StructField("rlp", StringType),
    StructField("rlc", StringType),
    StructField("a", StringType),
    StructField("b", StringType),
    StructField("prob", StringType)
  ))

  private val _originSchema = StructType(Seq(
    StructField("a", StringType),
    StructField("b", StringType)
  ))

  private val _noiseSchema = StructType(Seq(
    StructField("rlp", StringType),
    StructField("rlc", StringType)
  ))

  def originSchema: StructType = _originSchema

  def noiseSchema: StructType = _noiseSchema

  def rawDataSchema: StructType = _rawDataSchema

  def eStepInitSchema: StructType = _eStepInitSchema
}
