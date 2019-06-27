package com.batch.hadoopcompatibility

import org.apache.hadoop.io.{LongWritable, Text}
import org.apache.hadoop.mapreduce.Mapper

/**
  *
  * TODO
  */
object MyMapper extends Mapper[LongWritable,Text,Text,LongWritable]{

}
