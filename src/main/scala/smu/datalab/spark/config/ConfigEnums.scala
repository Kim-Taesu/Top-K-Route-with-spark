package smu.datalab.spark.config

object ConfigEnums {
  val ORIGIN_DATA: String = "origin_data"
  val NOISE_DATA: String = "noise_data"
  val E_STEP_INIT_DATA: String = "e_step_init_data"
  val TOP_K_DATA: String = "top_k_data"
  val ORIGIN_CASE: String = "origin_case"

  val SAMPLE: String = "sample"
  val MILLION: String = "1m"
  val TEN_MILLION: String = "10m"
  val ALL: String = "all"

  val P_VALUE: String = "pValue"
  val EPSILON: String = "epsilon"
  val DEST_CODE_LIST: String = "destCode"
  val SAVE_FILE_FORMAT: String = "saveFileFormat"

  val PARAMS: String = "params"
  val TEST: String = "test"
  val PATHS: String = "paths"
  val DIR: String = "dir"

  val TAXI_ID: String = "T_Link_ID"
  val DAY: String = "Day"
  val TIME: String = "Time"
  val DEST: String = "Dest"
  val START: String = "start"
  val END: String = "end"
  val WEATHER: String = "Weather"
  val CNT_ON: String = "CntOn"
  val CNT_OFF: String = "CntOff"
  val CNT_EMP: String = "CntEmp"
  val EMPTY: String = ""

  val HEADER: String = "header"
  val PROB: String = "prob"
  val RLP: String = "rlp"
  val EMPTY_STRING: String = ""
  val RLC: String = "rlc"
  val NOISE: String = "noise"
  val ORIGIN: String = "origin"
  val COUNT: String = "count"
  val TOTAL: String = "total"

  val RAW_DATA_DEST_COL_INDEX: Int = 3

  val CONFIG_PATH: String = "app-local.conf"

  val NO_EXIST: String = "0"
  val EXIST: String = "1"

  val TAXI_DATA_FRAME_COLS: Seq[String] = Seq(TAXI_ID, DAY, TIME, DEST)
}
