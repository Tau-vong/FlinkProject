package com.batch.transformations

import java.lang

import org.apache.flink.api.common.functions.{GroupCombineFunction, GroupReduceFunction}
import org.apache.flink.util.Collector

import collection.JavaConverters._
import scala.collection.mutable

/**
  *
  * TODO
  */
//Combinable GroupReduceFunctions
//In contrast to a reduce function, a group-reduce
// function is not implicitly combinable.
// In order to make a group-reduce function combinable
// it must implement the GroupCombineFunction interface.
//Important: The generic input and output types of the
// GroupCombineFunction interface must be equal to the
// generic input type of the GroupReduceFunction as shown
// in the following example:
class MyCombinableGroupReducer
  extends GroupReduceFunction[(String,Int),(String,Int)]
    with GroupCombineFunction[(String,Int),(String,Int)]{
  override def reduce(
                       iterable: lang.Iterable[(String, Int)],
                       collector: Collector[(String, Int)]): Unit = {
    for(in <- iterable.asScala){
      collector.collect((in._1 , in._2))
    }
  }

  override def combine(
                        iterable: lang.Iterable[(String, Int)],
                        collector: Collector[(String, Int)]): Unit = {
    val map = new mutable.HashMap[String , Int]()
    var num = 0
    var s = ""
    for(in <- iterable.asScala){
      num += in._2
      s = in._1
    }
    collector.collect((s , num))
  }
}
