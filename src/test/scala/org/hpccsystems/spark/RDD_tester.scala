package org.hpccsystems.spark

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.hpccsystems.ws.client.HPCCWsDFUClient
import org.hpccsystems.ws.client.utils.Connection
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo
import org.hpccsystems.spark.temp.DFUFilePartsOnClusterInfo
import org.hpccsystems.spark.temp.DFUFilePartInfo
import org.hpccsystems.spark.temp.FilePartsFactory


object RDD_tester {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark HPCC test")
    conf.setMaster("local")
    conf.setSparkHome("/Users/holtjd/WorkArea/spark-2.2.0-bin-hadoop2.7")
//    conf.setJars(List("/Users/holtjd/.m2/repository/org/hpccsystems/wsclient/1.3.0-SNAPSHOT.jar",
    conf.setJars(List(
        "/Users/holtjd/Repositories/HPCC-JAPIs/wsclient/target/wsclient-1.3.0-SNAPSHOT-jar-with-dependencies.jar",
        "/Users/holtjd/Repositories/Spark-HPCC/target/spark-hpcc-t0.jar"))
    println("Have set configuration")
    val spark = new SparkContext(conf)
    println("Have Spark Context")
    val protocol :String = "http"
    val targetHost : String = "10.239.40.2"
    val targetPort : String = "8010"
    val dsname : String = "~thor::jdh::japi_test"
    val cluster_name : String = ""
    val conn = new Connection(protocol, targetHost, targetPort)
    conn.setUserName("jholt")
    conn.setPassword("")
    val hpcc = HPCCWsDFUClient.get(conn)
    println("Have DFU Client")
    val fd = hpcc.getFileDetails(dsname, cluster_name)
    val dfu_parts = FilePartsFactory.makePartsOnCluster(fd)(0).getDFUFileParts
    val base_name = fd.getDir + "/" + fd.getFilename
    val parts = FilePart.makeFileParts(fd.getNumParts, base_name, dfu_parts)
    for (p <- parts) {
      println(p.primary_ip + "/" + p.secondary_ip + "  "
          + p.base_name + " " + p.this_part + "_of_" + p.num_parts
          + " size=" + p.part_size)
    }
    var mydefs = FieldDef.define_record(fd)
    var myRDD = new HpccRDD(spark, parts, mydefs);
    var rec_iter = myRDD.toLocalIterator
    while (rec_iter.hasNext) {
      val rec = rec_iter.next
      println(rec.get(0) + " " + rec.get(1))
    }
  }
}