/**
 *
 */
package org.hpccsystems.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import org.hpccsystems.spark.HpccFile;
import java.util.Arrays;
//


/**
 * Test from to test RDD by reading the data.
 * @author holtjd
 *
 */
public class RDDTest {
  public static void main(String[] args) throws Exception {
    String hpcc_ip = "10.239.40.2";
    String hpcc_port = "8010";
    String hpcc_file = "~thor::testdata::iris";
    //String hpcc_file = "~THOR::JDH::JAPI_FIXED";
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
    HpccFile hpcc = new HpccFile(hpcc_file, protocol, hpcc_ip, hpcc_port, "", "");
    System.out.println("Getting file parts");
    FilePart[] parts = hpcc.getFileParts();
    System.out.println("Getting record definition");
    RecordDef rd = hpcc.getRecordDefinition();
    System.out.println(rd.toString());
    System.out.println("Creating RDD");
    HpccRDD myRDD = new HpccRDD(sc, parts, rd);
    System.out.println("Getting local iterator");
    scala.collection.Iterator<Record> rec_iter = myRDD.toLocalIterator();
    while (rec_iter.hasNext()) {
      Record rec = rec_iter.next();
      System.out.println(rec.toString());
    }
    System.out.println("End of run");
  }
}
