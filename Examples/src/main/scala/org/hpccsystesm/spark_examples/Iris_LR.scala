package org.hpccsystesm.spark_examples

import org.hpccsystems.spark.HpccFile
import org.hpccsystems.spark.HpccRDD
import org.hpccsystems.spark.thor.RemapInfo
import org.apache.spark.mllib.classification.LogisticRegressionWithLBFGS
import org.apache.spark.mllib.evaluation.MulticlassMetrics
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext

object Iris_LR {
  def main(args: Array[String]) {
    val hpcc_protocol = "http"
    val hpcc_ip = "10.239.40.2"
    val hpcc_port = "8010"
    val hpcc_file = "~thor::testdata::iris"
    val hpcc_jar = "/Users/holtjd/Repositories/Spark-HPCC/target/spark-hpcc-t0.jar"
    val japi_jar = "/Users/holtjd/WorkArea/wsclient-2.0.0-SNAPSHOT-jar-with-dependencies.jar"
    val jar_list = Array(hpcc_jar, japi_jar)
    // Spark setup
    val conf = new SparkConf().setAppName("Iris_Spark_HPCC")
    conf.setMaster("local[2]")
    conf.setSparkHome("/Users/holtjd/WorkArea/spark-2.2.0-bin-hadoop2.7")
    conf.setJars(jar_list)
    val sc = new SparkContext(conf)
    //
    val ri = new RemapInfo(20, "10.240.37.108")
    val hpcc = new HpccFile(hpcc_file, hpcc_protocol, hpcc_ip, hpcc_port, "", "", ri)
    val myRDD = hpcc.getRDD(sc)
    val names = Array("petal_length","petal_width", "sepal_length", "sepal_width")
    val lpRDD = myRDD.makeMLLibLabeledPoint("class", names)
    val lr = new LogisticRegressionWithLBFGS().setNumClasses(3)
    val iris_model = lr.run(lpRDD)
    val predictionAndLabel = lpRDD.map {case LabeledPoint(label, features) =>
      val prediction = iris_model.predict(features)
      (prediction, label)
    }
    val metrics = new MulticlassMetrics(predictionAndLabel)
    println("Confusion matrix:")
    println(metrics.confusionMatrix)
  }
}