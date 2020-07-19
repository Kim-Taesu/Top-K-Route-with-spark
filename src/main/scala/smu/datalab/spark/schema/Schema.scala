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

  def rawDataSchema: StructType = _rawDataSchema
}
