package com.batch.hadoopcompatibility

import java.util

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapred.{JobConf, OutputCollector, Reducer, Reporter}

/**
  *
  * TODO
  */
object MyReducer extends Reducer[Text,LongWritable,Text,LongWritable]{


  override def reduce(k2: Text, iterator: util.Iterator[LongWritable], outputCollector: OutputCollector[Text, LongWritable], reporter: Reporter): Unit = {

  }

  override def close(): Unit = {

  }

  override def configure(jobConf: JobConf): Unit = {

  }
}
