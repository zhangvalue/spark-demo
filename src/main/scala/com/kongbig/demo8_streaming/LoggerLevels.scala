package com.kongbig.demo8_streaming

import org.apache.log4j.{Level, Logger}
import org.apache.spark.Logging

/**
  * Describe: 
  * Author:   kongbig
  * Data:     2018/1/14.
  */
object LoggerLevels extends Logging {
  def setStreamingLogLevels() {
    val log4jInitialized = Logger.getRootLogger.getAllAppenders.hasMoreElements
    if (!log4jInitialized) {
      logInfo("Setting log level to [WARN] for streaming example." +
        " To override add a custom log4j.properties to the classpath.")
      Logger.getRootLogger.setLevel(Level.WARN)
    }
  }
}
