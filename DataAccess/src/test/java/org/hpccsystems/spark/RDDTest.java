package org.hpccsystems.spark;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS;
import org.apache.spark.mllib.classification.LogisticRegressionModel;
import org.apache.spark.mllib.evaluation.MulticlassMetrics;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import scala.collection.Seq;
import scala.collection.JavaConverters;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import org.hpccsystems.spark.HpccFile;
import org.hpccsystems.spark.thor.RemapInfo;
import java.util.Arrays;
//


/**
 * Test from to test RDD by reading the data and running Logistic Regression.
 * @author holtjd
 *
 */
public class RDDTest {
  public static void main(String[] args) throws Exception {
    String hpcc_ip = "10.240.37.76";
    String hpcc_port = "8010";
    String hpcc_file = "~thor::test::iris";
    //String hpcc_file = "~THOR::JDH::JAPI_FIXED";
    String protocol = "http";
    //
    SparkConf conf = new SparkConf().setAppName("Spark HPCC test");
    conf.setMaster("local[2]");
    conf.setSparkHome("/Users/holtjd/WorkArea/spark-2.2.1-bin-hadoop2.7");
    String japi_jar = "/Users/holtjd/WorkArea/wsclient-2.0.0-SNAPSHOT-jar-with-dependencies.jar";
    String this_jar = "/Users/holtjd/Repositories/Spark-HPCC/target/spark-hpcc-t0.jar";
    java.util.List<String> jar_list = Arrays.asList(this_jar, japi_jar);
    Seq<String> jar_seq = JavaConverters.iterableAsScalaIterableConverter(jar_list).asScala().toSeq();;
    conf.setJars(jar_seq);
    System.out.println("Spark configuration set");
    SparkContext sc = new SparkContext(conf);
    System.out.println("Spark context available");
    //
    RemapInfo ri = new RemapInfo(20, "10.240.37.108");
    HpccFile hpcc = new HpccFile(hpcc_file, protocol, hpcc_ip, hpcc_port, "", "", ri);
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
    System.out.println("Completed output of Record data");
    System.out.println("Convert to labeled point and run logistic regression");
    String[] names = {"petal_length","petal_width", "sepal_length", "sepal_width"};
    RDD<LabeledPoint> lpRDD = myRDD.makeMLLibLabeledPoint("class", names);
    LogisticRegressionWithLBFGS lr  = new LogisticRegressionWithLBFGS();
    lr.setNumClasses(3);
    LogisticRegressionModel iris_model = lr.run(lpRDD);
    System.out.println(iris_model.toString());
    System.out.println("Generate confusion matrix");
    Function<LabeledPoint, Tuple2<Object, Object>> my_f
      = new Function<LabeledPoint, Tuple2<Object, Object>>() {
      static private final long serialVersionUID = 1L;
      public Tuple2<Object, Object> call(LabeledPoint lp) {
        Double label = new Double(lp.label());
        Double predict = new Double(iris_model.predict(lp.features()));
        return new Tuple2<Object, Object>(predict, label);
      }
    };
    ClassTag<LabeledPoint> typ = ClassTag$.MODULE$.apply(LabeledPoint.class);
    JavaRDD<LabeledPoint> lpJavaRDD = new JavaRDD<LabeledPoint>(lpRDD, typ);
    RDD<Tuple2<Object, Object>> predAndLabelRDD = lpJavaRDD.map(my_f).rdd();
    MulticlassMetrics metrics = new MulticlassMetrics(predAndLabelRDD);
    System.out.println("Confusion matrix:");
    System.out.println(metrics.confusionMatrix());
    System.out.println("End of run");
  }
}
