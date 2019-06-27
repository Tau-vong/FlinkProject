package com.batch.transformations

import java.lang

import org.apache.flink.api.common.functions.CoGroupFunction
import org.apache.flink.util.Collector

/**
  *
  * TODO
  */
class MyCoGroupFunc
  extends CoGroupFunction[(String,Int),(String,String),(String,String)]{
  override def coGroup(
                        iterable: lang.Iterable[(String, Int)],
                        iterable1: lang.Iterable[(String, String)],
                        collector: Collector[(String, String)]): Unit = {

  }
}
