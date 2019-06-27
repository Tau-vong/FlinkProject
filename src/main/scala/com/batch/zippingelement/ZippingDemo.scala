package com.batch.zippingelement

import org.apache.flink.api.scala._
import org.apache.flink.api.scala.utils.DataSetUtils

/**
  *
  * TODO
  */
object ZippingDemo {
  def main(args: Array[String]): Unit = {
    val benv=ExecutionEnvironment.getExecutionEnvironment
    benv.setParallelism(2)
    //zipWithIndex assigns consecutive labels to the elements,
    // receiving a data set as input and
    // returning a new data set of
    // (unique id, initial value) 2-tuple
    val input:DataSet[String]=benv.fromElements("a","b","c")
    val result:DataSet[(Long,String)]=input.zipWithIndex
    val result0:DataSet[(Long,String)]=input.zipWithUniqueId
    result.print()
    println("-----------------------------------------")
    result0.print()
  }

}
