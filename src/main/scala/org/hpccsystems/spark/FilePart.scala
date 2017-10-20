package org.hpccsystems.spark

import org.apache.spark.Partition
import org.hpccsystems.spark.temp.DFUFilePartInfo


class FilePart(val primary_ip : String,
    val secondary_ip : String,
    val base_name : String,
    val this_part : Int,
    val num_parts : Int,
    val part_size: Int)
  extends Partition {
  override def index = this_part-1
}

object FilePart {
  def sortByPart(p1:DFUFilePartInfo, p2:DFUFilePartInfo) = {
    p1.getId() < p2.getId() || (p1.getId()==p2.getId() && p1.getCopy() < p2.getCopy())
  }
  // Assumes that the number of copies is the same for all parts
  def makeFileParts(num_parts : Int,
      base_name : String,
      parts : Array[DFUFilePartInfo]) : Array[FilePart] = {
    val rslt = new Array[FilePart](num_parts)
    var copies = parts.size / num_parts
    for (ndx <- 0 to num_parts-1) {
      val fpi0 : DFUFilePartInfo = parts(ndx*copies)
      val fpi1 : DFUFilePartInfo = parts((ndx*copies)+1)
      rslt(ndx) = new FilePart(fpi0.getIp, fpi1.getIp, base_name,
                          fpi0.getId, num_parts, fpi0.getPartsize.toInt)
    }
    rslt
  }
}