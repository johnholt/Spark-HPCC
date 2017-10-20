package org.hpccsystems.spark


import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.spark.temp.DFUFilePartsOnClusterInfo;
import org.hpccsystems.spark.temp.DFUFilePartInfo;
import org.hpccsystems.spark.temp.FilePartsFactory;
import org.hpccsystems.ws.client.platform.DFURecordDefInfo;
import org.hpccsystems.ws.client.platform.DFUDataColumnInfo;
import org.hpccsystems.ws.client.platform.EclRecordInfo;
import org.hpccsystems.ws.client.utils.Connection;

import org.hpccsystems.spark

object DFU_tester {
  def main(args: Array[String]) : Unit = {
    val protocol :String = "http"
    val targetHost : String = "10.239.40.2"
    val targetPort : String = "8010"
    val dsname : String = "~thor::jdh::japi_test"
    val cluster_name : String = ""
    val conn = new Connection(protocol, targetHost, targetPort)
    conn.setUserName("jholt")
    conn.setPassword("")
    val hpcc = HPCCWsDFUClient.get(conn)
    val fd = hpcc.getFileDetails(dsname, cluster_name)
    println("File " + fd.getFilename() + " with " + fd.getNumParts() + " parts.");
    println("Ecl Record string: " + fd.getEcl())
    println("Column Info")
    var fields : java.util.ArrayList[DFUDataColumnInfo] = fd.getColumns();
    for (i <- 1 to fields.size()) print_it(1, fields.get(i-1))
    println("Column info above")
    var defs = FieldDef.define_record(fd)
    println(defs.toString)
    val dfu_parts = FilePartsFactory.makePartsOnCluster(fd)(0).getDFUFileParts
    val base_name = fd.getDir + "/" + fd.getFilename
    val parts = FilePart.makeFileParts(fd.getNumParts, base_name, dfu_parts)
    for (p <- parts) {
      println(p.primary_ip + "/" + p.secondary_ip + "  "
          + p.base_name + " " + p.this_part + "_of_" + p.num_parts
          + " size=" + p.part_size)
    }
  }
  def print_it(lvl : Int, info : DFUDataColumnInfo) {
    println(lvl.toString + ' ' + info.getColumnLabel() + " : " + info.getColumnType()
        + ":" + info.getColumnEclType());
    val child_cols = info.getChildColumns()
    for (i <- 1 to child_cols.size()) {
      print_it(lvl + 1, child_cols.get(i-1))
    }
  }
}