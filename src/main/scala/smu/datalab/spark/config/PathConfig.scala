package smu.datalab.spark.config

import com.typesafe.config.Config
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.util.Utils.makePath

case class PathConfig(config: Config) {
  val testSamplePath: String = config.getString(makePath(PATHS, TEST, SAMPLE))
  val test1MPath: String = config.getString(makePath(PATHS, TEST, MILLION))
  val test10MPath: String = config.getString(makePath(PATHS, TEST, TEN_MILLION))
  val testAllPath: String = config.getString(makePath(PATHS, TEST, ALL))
}
