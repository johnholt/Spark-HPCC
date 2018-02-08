package org.hpccsystems.spark;

import java.util.Iterator;

import org.hpccsystems.spark.thor.BinaryRecordReader;
import org.hpccsystems.spark.thor.FieldDef;

public class RecordTest {

  public static void main(String[] args) throws Exception{
    //String MyVM = "127.0.0.1";
    String ML_Dev = "10.239.40.2";
    //String testName = "~THOR::JDH::JAPI_TEST1";
    String testName = "~THOR::JDH::JAPI_TEST1a";
    //String testName = "~THOR::JDH::JAPI_FIXED";
    //String testName = "~THOR::TESTDATA::IRIS";
    HpccFile hpcc = new HpccFile(testName, "http", ML_Dev, "8010", "", "");
    System.out.println("Getting file parts");
    FilePart[] parts = hpcc.getFileParts();
    for (int i=0; i<parts.length; i++) {
      System.out.println(parts[i].getFilename() + ":"
              + parts[i].getPrimaryIP()+ ":"
              + parts[i].getSecondaryIP() + ": "
              + parts[i].getThisPart());
    }
    System.out.println("Getting record definition");
    RecordDef rd = hpcc.getRecordDefinition();
    FieldDef root_def = rd.getRootDef();
    Iterator<FieldDef> iter = root_def.getDefinitions();
    while (iter.hasNext()) {
      FieldDef field = iter.next();
      System.out.println(field.toString());
    }
    System.out.println("Reading records");
    BinaryRecordReader brr = new BinaryRecordReader(parts[1], rd);
    while (brr.hasNext()) {
      Record rec = brr.getNext();
      System.out.println(rec.toString());
    }
    System.out.println("Completed read, end of test");
  }

}
