package org.hpccsystems.spark;

import java.util.Iterator;

/**
 * Test the access for information on a distributed file on a THOR cluster.
 * @author John Holt
 *
 */
public class HpccFileTest {

  public static void main(String[] args) throws Exception {
    //String MyVM = "127.0.0.1";
    String ML_Dev = "10.239.40.2";
    //String testName = "~THOR::JDH::JAPI_TEST1";
    //String testName = "~THOR::JDH::JAPI_TEST2";
    //String testName = "~THOR::JDH::JAPI_FIXED";
    String testName = "~THOR::TESTDATA::IRIS";
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
    System.out.println("Reading block");
    org.hpccsystems.spark.data.PlainConnection pc
          = new org.hpccsystems.spark.data.PlainConnection(parts[1], rd);
    System.out.print("Transaction : ");
    System.out.println(pc.getTrans());
    System.out.println(pc.getIP());
    System.out.println(pc.getFilename());
    byte[] block = pc.readBlock();
    StringBuilder sb = new StringBuilder();
    sb.append("Handle ");
    sb.append(pc.getHandle());
    sb.append(", data length=");
    sb.append(block.length);
    System.out.println(sb.toString());
    for (int i=0; i<block.length; i+=16) {
      sb.delete(0, sb.length());
      for (int j=0; j<16 && i+j<block.length; j++) {
        sb.append(String.format("%02X ", block[i+j]));
        sb.append(" ");
      }
      System.out.println(sb.toString());
    }
    System.out.println("End test");
  }
}
