package org.hpccsystems.spark;

import java.util.Iterator;

import org.hpccsystems.spark.thor.BinaryRecordReader;
import org.hpccsystems.spark.thor.FieldDef;
import org.hpccsystems.spark.thor.RemapInfo;

public class RecordTest {

  public static void main(String[] args) throws Exception{
    //String MyVM = "127.0.0.1";
    //String ML_Dev = "10.239.40.2";
    String ML_Dev = "10.240.37.76";
    //String testName = "~THOR::JDH::JAPI_TEST1";
    //String testName = "~THOR::JDH::JAPI_TEST1a";
    //String testName = "~THOR::JDH::JAPI_FIXED";
    String testName = "~THOR::TEST::IRIS";
    RemapInfo ri = new RemapInfo(20,"10.240.37.108");
    HpccFile hpcc = new HpccFile(testName, "http", ML_Dev, "8010", "", "", ri);
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
    for (int i=0; i<parts.length; i++) {
      System.out.println("Reading records from part index " + i);
      try {
        BinaryRecordReader brr = new BinaryRecordReader(parts[i], rd);
        while (brr.hasNext()) {
          Record rec = brr.getNext();
          System.out.println(rec.toString());
        }
        System.out.println("completed part at index "+i);
      } catch (Exception e) {
        StringBuilder sb = new StringBuilder();
        sb.append("Failed for part ");
        sb.append(parts[i].getThisPart());
        sb.append(" to ");
        sb.append(parts[i].getPrimaryIP());
        sb.append(":");
        sb.append(parts[i].getClearPort());
        sb.append(" with error ");
        sb.append(e.getMessage());
        System.out.println(sb.toString());
      }
    }
    System.out.println("Completed read, end of test");
  }

}
