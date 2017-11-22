/**
 *
 */
package org.hpccsystems.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import org.hpccsystems.spark.HpccFiles;
import java.util.Arrays;
//
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.utils.Connection;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartsOnClusterInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;


/**
 * Test from to test RDD by reading the data.
 * @author holtjd
 *
 */
public class RDDTest {
  public static void main(String[] args) throws Exception {
    String hpcc_ip = "10.239.40.2";
    String hpcc_port = "8010";
    String hpcc_file = "~thor::jdh::japi_test1";
    String protocol = "http";
    //
    SparkConf conf = new SparkConf().setAppName("Spark HPCC test");
    conf.setMaster("local[2]");
    conf.setSparkHome("/Users/holtjd/WorkArea/spark-2.2.0-bin-hadoop2.7");
    String japi_jar = "/Users/holtjd/Repositories/HPCC-JAPIs/wsclient/target"
        + "/wsclient-1.3.0-SNAPSHOT-jar-with-dependencies.jar";
    String this_jar = "/Users/holtjd/Repositories/Spark-HPCC/target/spark-hpcc-t0.jar";
    java.util.List<String> jar_list = Arrays.asList(this_jar);
    Seq<String> jar_seq = JavaConverters.iterableAsScalaIterableConverter(jar_list).asScala().toSeq();;
    conf.setJars(jar_seq);
    System.out.println("Spark configuration set");
    SparkContext sc = new SparkContext(conf);
    System.out.println("Spark context available");
    //
    Connection conn = new Connection(protocol, hpcc_ip, hpcc_port);
    conn.setUserName("jholt");
    conn.setPassword("");
    HPCCWsDFUClient hpcc =HPCCWsDFUClient.get(conn);
    DFUFileDetailInfo fd = hpcc.getFileDetails(hpcc_file,  "");
    DFUFilePartInfo[] dfu_parts = fd.getDFUFilePartsOnClusters()[0].getDFUFileParts();
    //String base_name = fd.getDir() + "/" + fd.getFilename();
    FilePart[] parts = FilePart.makeFileParts(fd.getNumParts(), fd.getDir(),
        fd.getFilename(), fd.getPathMask(), dfu_parts);
    System.out.println("Number of file part is: " + parts.length);
    for (FilePart fp : parts) {
      System.out.println("p:" + fp.getPrimaryIP() + "; s:" + fp.getSecondaryIP()
              + "; part=" + fp.getThisPart() + " of " + fp.getNumParts()
              + "; size=" + fp.getPartSize());
    }
    HpccRDD myRDD = new HpccRDD(sc, parts, new RecordDef());
    System.out.println("Getting local iterator");
    scala.collection.Iterator<Record> rec_iter = myRDD.toLocalIterator();
    while (rec_iter.hasNext()) {
      Record rec = rec_iter.next();
      FieldContent f1 = rec.getFieldContent("F1");
      FieldContent f2 = rec.getFieldContent("F2");
      System.out.println(f1.asString() + " / " + f2.asString());
    }
    System.out.println("End of run");
  }
}
