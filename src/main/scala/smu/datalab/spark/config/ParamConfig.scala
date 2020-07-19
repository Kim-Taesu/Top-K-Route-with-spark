package smu.datalab.spark.config

import java.util

import com.typesafe.config.Config
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.util.Utils._

import scala.collection.JavaConverters._

case class ParamConfig(config: Config) {
  val epsilon: Double = config.getDouble(makePath(PARAMS, EPSILON))
  val pValue: Double = config.getDouble(makePath(PARAMS, P_VALUE))
  val threshold: Double = config.getDouble(makePath(PARAMS, THRESHOLD))
  val destCodeList: util.List[String] = config.getStringList(makePath(PARAMS, DEST_CODE_LIST))
  val saveFileFormat: String = config.getString(makePath(PARAMS, SAVE_FILE_FORMAT))
  val qValue: Double = 1.0 / Math.exp(epsilon)
  val iteratorCnt: Int = config.getInt(makePath(PARAMS, ITERATOR_CNT))

  def getDestCodeList(size: Int): List[String] = {
    this.destCodeList.asScala.toList.take(size)
  }
}
