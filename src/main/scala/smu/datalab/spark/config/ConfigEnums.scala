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
  val THRESHOLD: String = "threshold"
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
  val ITERATOR_CNT: String = "iteratorCnt"

  val E_STEP_PROB: String = "e_step_prob"
  val E_STEP_RESULT: String = "e_step_result"
  val E_STEP_RESULT_SUM: String = "e_step_result_sum"
  val E_STEP_SUM: String = "e_step_sum"
  val M_STEP_TMP: String = "m_step_tmp"

  val ID: String = "id"
  val PRE_THETA: String = "pre_theta"
  val THETA: String = "theta"
  val HEADER: String = "header"
  val PROB: String = "prob"
  val RLP: String = "rlp"
  val EMPTY_STRING: String = ""
  val RLC: String = "rlc"
  val NOISE: String = "noise"
  val ORIGIN: String = "origin"
  val COUNT: String = "count"
  val TOTAL: String = "total"
  val NOISE_START: String = s"${NOISE}_$START"
  val NOISE_END: String = s"${NOISE}_$END"
  val ORIGIN_START: String = s"${ORIGIN}_$START"
  val ORIGIN_END: String = s"${ORIGIN}_$END"
  val ORIGIN_PROB: String = s"${ORIGIN}_$PROB"
  val NOISE_PROB: String = s"${NOISE}_$PROB"

  val CSV: String = "csv"

  val DEST_SIZE: String = "destSize"
  val SAVE: String = "save"
  val ORIGIN_DATA_PATH: String = "originDataPath"
  val NOISE_DATA_PATH: String = "noiseDataPath"
  val E_STEP_INIT_DATA_PATH: String = "eStepInitDataPath"
  val RESULT_DATA_PATH: String = "resultDataPath"

  val RAW_DATA_DEST_COL_INDEX: Int = 3

  val CONFIG_PATH: String = "app-config.conf"

  val NO_EXIST: String = "0"
  val EXIST: String = "1"

  val TAXI_DATA_FRAME_COLS: Seq[String] = Seq(TAXI_ID, DAY, TIME, DEST)
}
