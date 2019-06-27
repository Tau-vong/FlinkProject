package com.batch.hadoopcompatibility


import org.apache.flink.api.scala._
import org.apache.flink.api.scala.hadoop.mapred.HadoopOutputFormat
import org.apache.hadoop.io.{IntWritable, LongWritable, Text}
import org.apache.flink.hadoopcompatibility.HadoopInputs
import org.apache.flink.hadoopcompatibility.mapred.{HadoopMapFunction, HadoopReduceCombineFunction}
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred._



/**
  *
  * TODO
  */
//flink supports all hadoop writable and
// writablecomparable data type out-of-the-box
object FlinkHadoopDemo {
  def main(args: Array[String]): Unit = {
    val benv=ExecutionEnvironment.getExecutionEnvironment
    // input:DataSet[(LongWritable,Text)]---为什么报错？
    //调用hadoop标准输入
    val input= benv.createInput(HadoopInputs.readHadoopFile(
      new TextInputFormat, classOf[LongWritable], classOf[Text], ""
    ))

    //调用hadoop标准输出
    val hadoopResult:DataSet[(Text,IntWritable)]=
      input.map(x=>(x.f1,new IntWritable(1)))
    val hadoopOF=new HadoopOutputFormat[Text,IntWritable](
      new TextOutputFormat[Text,IntWritable],new JobConf()
    )
    hadoopOF.getJobConf.set("mapred.textoutputformat.separator"," ")
    FileOutputFormat.setOutputPath(hadoopOF.getJobConf,
      new Path(""))
    hadoopResult.output(hadoopOF)

    //使用hadoop的mapreduce
    /*
    input.flatMap(new HadoopMapFunction[LongWritable,Text,Text,LongWritable](
      MyMapper()
    )).groupBy(0)
      .reduceGroup(
        new HadoopReduceCombineFunction[Text,LongWritable,Text,LongWritable](
        MyReducer(),MyReducer()
      ))
    */

  }

}
