package smu.datalab.spark.config

import com.typesafe.config.Config
import smu.datalab.spark.config.ConfigEnums._
import smu.datalab.spark.util.Utils.makePath

case class PathConfig(config: Config) {
  val testSamplePath: String = config.getString(makePath(PATHS, TEST, SAMPLE))
  val test1MPath: String = config.getString(makePath(PATHS, TEST, MILLION))
  val test10MPath: String = config.getString(makePath(PATHS, TEST, TEN_MILLION))
  val testAllPath: String = config.getString(makePath(PATHS, TEST, ALL))

  val originDataPath: String = config.getString(makePath(PATHS, SAVE, ORIGIN_DATA_PATH))
  val noiseDataPath: String = config.getString(makePath(PATHS, SAVE, NOISE_DATA_PATH))
  val eStepInitDataPath: String = config.getString(makePath(PATHS, SAVE, E_STEP_INIT_DATA_PATH))
}
