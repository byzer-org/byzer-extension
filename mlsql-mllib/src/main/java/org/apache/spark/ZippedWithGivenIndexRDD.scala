package org.apache.spark

import org.apache.spark.rdd.{RDD, ZippedWithIndexRDD, ZippedWithIndexRDDPartition}
import org.apache.spark.util.Utils

import scala.reflect.ClassTag

/**
 * 27/07/2022 hellozepp(lisheng.zhanglin@163.com)
 *
 * Represents an RDD zipped with its element indices. The user can specify a starting value for indices.
 *
 * @param prev       parent RDD
 * @param startIndex a starting value for indices; The default value is 0, the behavior is consistent with ZippedWithIndexRDD.
 * @tparam T parent RDD item type
 */
class ZippedWithGivenIndexRDD[T: ClassTag](prev: RDD[T], startIndex: Long = 0) extends ZippedWithIndexRDD[T](prev: RDD[T]) {

  @transient private val startIndices: Array[Long] = {
    val n = prev.partitions.length
    if (n == 0) {
      Array.empty
    } else if (n == 1) {
      Array(startIndex)
    } else {
      val longs = prev.context.runJob(
        prev,
        Utils.getIteratorSize _,
        0 until n - 1 // do not need to count the last partition
      ).scanLeft(startIndex)(_ + _)
      longs
    }
  }

  override def getPartitions: Array[Partition] = {
    firstParent[T].partitions.map(x => new ZippedWithIndexRDDPartition(x, startIndices(x.index)))
  }
}
