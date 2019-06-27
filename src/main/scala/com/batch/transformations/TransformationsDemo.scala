package com.batch.transformations

import org.apache.flink.api.common.operators.Order
import org.apache.flink.api.scala._
import org.apache.flink.util.Collector
//asScala
import collection.JavaConverters._


object TransformationsDemo {
  case class one(key1:String,value1:Int)
  case class two(key2:String,value2:String)
  def main(args: Array[String]): Unit = {
    val env=ExecutionEnvironment.getExecutionEnvironment
    val ds=env.readTextFile("/Users/tao.wang" +
      "/IdeaProjects/FlinkProject/src/main/" +
      "resources/file/1.txt")
      .map(_.split(","))
      .map(x=>one(x(0),x(1).toInt))
    val ds0=env.fromCollection(List(
      ("a","1"),
      ("b","2"),
      ("c","3"),
      ("d","4"),
      ("e","5")
    ))
      .map(x=>two(x._1,x._2))


    val ds1=env.readTextFile("/Users/tao.wang" +
      "/IdeaProjects/FlinkProject/src/main/" +
      "resources/file/2.txt")


    val ds2=env.readTextFile("/Users/tao.wang" +
      "/IdeaProjects/FlinkProject/src/main/" +
      "resources/file/3.txt")


    val ds3=env.readTextFile("/Users/tao.wang" +
      "/IdeaProjects/FlinkProject/src/main/" +
      "resources/file/4.txt")
/*

    //～ 1 ～ 简单用法
    ds.groupBy(0)
      .sum(1)
      .print()
    println("----------")



    //～ 2 ～ mapPartition
    //mapPartition的高效用法
    //mapPartition与map相比：前者每个task执行一次function，而map每条数据执行一次function
    //前者是对rdd中的每个分区的迭代器进行操作；后者是对rdd中的每一个元素进行操作
    //前者比后者效率高，前者大量数据会导致oom异常
    ds.map(x=>(x.key1,x.value1))
      .mapPartition(v=>new MyCustomIterator(v))
      .groupBy(0)
      .sum(1)
      .print()
    println("----------")



    //～ 3 ～ join
    //相当于内连接，只返回键相等的数据，equalTo{//写函数，以自己想要的格式写出数据}
    ds.join(ds0)
      .where("key1")
      .equalTo("key2"){
//        (x,y,out:Collector[(String,String,String)])=>
//          if(x.key1==y._1){
//            out.collect((x.key1,x.value1.toString,y._2))
//          }else{
//            out.collect((x.key1,x.value1.toString,null))
//          }
        (x,y)=>(x.key1,x.value1,y.value2)
      }
      .print()
    println("+++++++++++++++++++++")



    //～ 4 ～ rightOuterJoin
    //相当于右连接，已经将右表所有行都存储了，想打印需要判断左表是否有空值，做判断然后输出自己想要的格式
    ds.rightOuterJoin(ds0)
      .where("key1")
      .equalTo("key2"){
        (x,y)=>(if(x==null) -1 else x.value1 ,y.key2,y.value2)
      }
      .print()
    println("+++++++++++++++++++++")



    //～ 5 ～ leftOuterJoin
    //相当于左连接，已经将左表的所有行都存储了，输出时需要判断右表是否为空
    ds.leftOuterJoin(ds0)
      .where("key1")
      .equalTo("key2"){
        (x,y)=>(x.key1,x.value1,if(y==null) -1 else y.value2)
      }
      .print()
    println("+++++++++++++++++++++")



    //～ 6 ～ fullOuterJoin
    //相当于全连接，左右表都可能有不存在的行，所以需要判断再输出
    ds.fullOuterJoin(ds0)
      .where("key1")
      .equalTo("key2"){
        (x,y)=>
          if(x!=null && y!=null){
            (x.key1,x.value1,y.value2)
          }else{
            (if(x==null) -1 else x.key1+","+x.value1 ,if(y==null) -1 else y.key2+","+y.value2)
          }
      }.print()
    println("+++++++++++++++++++++")



    //～ 7 ～ filter/map/flatmap/groupby/sum/reduce/reducegroup/
    //sortPatition/first-n/minBy/maxBy/union/cross
    //aggregate---相当于sum，min，max等
    //部分算子既可以用于分组后的dataset，也可以用于完整的dataset（full dataset）
    println("=====================")
    //用map切完之后是多个数组，而用flatmap切完之后是一个大数组
    ds1.flatMap(x=>x.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .filter(_._2 % 4 == 0)
      .print()
    println("=====================")
    ds1.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .reduce((x,y)=>(x._1,y._2 + x._2))//xy指代前一个结果和后一个结果
      .print()
    println("=====================")
    ds1.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .aggregate(org.apache.flink.api.java.aggregation.Aggregations.SUM,1)
//      .and(org.apache.flink.api.java.aggregation.Aggregations.MIN,1)
      .print()
    println("=====================")
    ds1.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .minBy(1)
      .print()
    println("=====================")
    ds1.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .sum(1)
      .sortPartition(1,Order.DESCENDING)
      .first(5)
      .print()
    println("=====================")
    //连接同类型的数据源
    ds.map(x=>x.key1).union(ds1).print()
    println("=====================")
    ds.cross(ds0).print()
    println("=====================")


    //～ 8 ～GroupReduce/CoGroup
    ds.coGroup(ds0)
      .where("key1")
      .equalTo("key2")
      .print()
    println("$$$$$$$$$$$$$$$$$$$$$$$$")
    //里面是迭代器
    //cogroup侧重于group，是对同一组key上的两组集合进行操作
    //cogroup与jion基本相同，但不一样的在于如果未能找到新来
    // 数据与另一流在window中存在匹配数据，仍会将其输出
    // 而join侧重是pair，是对同一个key上的每对元素进行操作，
    // 类似于inner join，按照一定条件分别取出两个流中匹配的元素，
    // 返回给下游处理
    // join是cogroup的特例
    //两者只能在window中使用（批处理是流处理的特例，所以也能用）
    ds.map(x=>(x.key1,x.value1))
      .coGroup(ds0.map(x=>(x.key2,x.value2)))
      .where(0)
      .equalTo(0){
        (l,r)=>if(l.nonEmpty && r.nonEmpty) {l.min}
          else{}
      }
      .print()
    println("$$$$$$$$$$$$$$$$$$$$$$$$")
    //按key分组，每个key下的元素放到一个迭代器中进行后续操作
    //GroupReduce on DataSet Grouped by Field Position Keys
    // (Tuple DataSets only)
    //先分组reduce，然后在做整体的reduce；这样做的好处就是可以减少网络IO
    ds1.flatMap(_.split(" "))
      .map((_,1))
      .groupBy(0)
      .reduceGroup{
        in=>in.reduce((x,y)=>(x._1,x._2 + y._2))
      }
      .print()
    println("$$$$$$$$$$$$$$$$$$$$$$$$")
    //可以实现按key分组，分组后排序进行操作
    // GroupReduce on sorted groups
    ds2.map(x=>x.split(","))
      .map(x=>(x(0),x(1)))
      .groupBy(0)
      .sortGroup(1,Order.DESCENDING)
      .reduceGroup{
        in=>in.max
      }
      .print()
    println("$$$$$$$$$$$$$$$$$$$$$$$$")


*/
    //~~~ todo ~~~ combineGroup结合reduceGroup
    //使用之前的group操作，
    // 比如：reduceGroup或者GroupReduceFuncation；
    // 这种操作很容易造成内存溢出；
    // 因为要一次性把所有的数据一步转化到位，所以需要足够的内存支撑，
    // 如果内存不够的情况下，那么需要使用combineGroup；
    // combineGroup在分组数据集上应用GroupCombineFunction。
    // GroupCombineFunction类似于GroupReduceFunction，
    // 但不执行完整的数据交换。
    // 【注意】：使用combineGroup可能得不到完整的结果而是部分的结果

//    ds3.map((_,1)).groupBy(0).combineGroup {
//      (in:Iterator[(String,Int)], out: Collector[(String,Int)]) =>
//        val a=in.mkString
//        out.collect((a,0))
//    }
//      .print()
//    import scala.collection.JavaConversions._
//    import collection.JavaConverters._
//
    ds3.groupBy(Tuple1(_))
      .combineGroup{
        (words:Iterator[String], out: Collector[(String, Int)]) =>
          var key: String = null
          var count = 0

          for (word <- words) {
            key = word
            count += 1
          }
          out.collect((key, count))
      }
      .reduceGroup{
        (words:Iterator[(String,Int)], out: Collector[(String, Int)]) =>
          var key: String = null
          var sum = 0

          for (word <- words) {
            key = word._1
            sum = word._2
          }
          out.collect((key, sum))
      }
      .print()
    //结果是(spark,3)---为什么？
    println("$$$$$$$$$$$$$$$$$$$$$$$$")

  }
}