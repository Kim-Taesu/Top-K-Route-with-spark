package smu.datalab.spark.config

import java.util

import com.typesafe.config.Config
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.util.Utils._

import scala.collection.JavaConverters._

case class ParamConfig(config: Config) {
  val epsilon: Double = config.getDouble(makePath(PARAMS, EPSILON))
  val pValue: Double = config.getDouble(makePath(PARAMS, P_VALUE))
  val destCodeList: util.List[String] = config.getStringList(makePath(PARAMS, DEST_CODE_LIST))
  val saveFileFormat: String = config.getString(makePath(PARAMS, SAVE_FILE_FORMAT))
  val qValue: Double = 1.0 / Math.exp(epsilon)

  def getDestCodeList(size: Int): List[String] = {
    this.destCodeList.asScala.toList.take(size)
  }
}
