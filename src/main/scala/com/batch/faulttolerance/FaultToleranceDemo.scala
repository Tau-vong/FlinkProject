package com.batch.faulttolerance

import org.apache.flink.api.scala.ExecutionEnvironment

/**
  *
  * TODO
  */
object FaultToleranceDemo {
  def main(args: Array[String]): Unit = {
    val benv=ExecutionEnvironment.getExecutionEnvironment
    //can set the execution retries and retry delay for each
    // program as follow
    benv.setNumberOfExecutionRetries(3)
    benv.getConfig.setExecutionRetryDelay(5000)
    //can also define default values for the number of execution
    // retries and the retry delay in the flink-conf.yaml

  }

}
