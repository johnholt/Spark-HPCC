package org.hpccsystems.spark

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.Partition
import org.apache.spark.TaskContext
import org.apache.spark.InterruptibleIterator
import org.apache.spark.Dependency

class HpccRDD(sc : SparkContext,
      val parts : Array[FilePart],
      val def_head : FieldDef
    ) extends RDD[Record](sc, Nil) {
  override def getPartitions() : Array[Partition] = {
    var rslt = new Array[Partition](parts.size)
    for (i <- 0 to parts.size-1) rslt(i)=parts(i)
    rslt
  }
  override def compute(sparkPart : Partition, ctx : TaskContext)
      : InterruptibleIterator[Record] = {
    val part = sparkPart.asInstanceOf[FilePart]
    val iter = new Iterator[Record] {
      val this_partition = part
      var pos = 0
      var moreData = part.part_size > pos
      override def hasNext() = moreData
      override def next() : Record = {
        if (!moreData) throw new java.util.NoSuchElementException("HPCC file partition")
        var rslt = Array[Any]("File part " + this_partition.this_part, "pos " + pos)
        pos += 100
        moreData = this_partition.part_size > pos
        new Record(Array("F1", "F2"), rslt)
      }
    }
    new InterruptibleIterator(ctx, iter)
  }
}