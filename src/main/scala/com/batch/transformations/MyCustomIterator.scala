package com.batch.transformations

/**
  *mapPartition的高效用法
  */
class MyCustomIterator(iter:Iterator[(String,Int)])
  extends Iterator[(String,Int)]{
  override def hasNext: Boolean = {
    iter.hasNext
  }

  override def next(): (String, Int) = {
    val cur=iter.next()
    (cur._1,cur._2*20)
  }
}
