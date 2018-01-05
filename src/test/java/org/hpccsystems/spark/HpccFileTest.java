package org.hpccsystems.spark;

import java.util.Iterator;

/**
 * Test the access for information on a distributed file on a THOR cluster.
 * @author John Holt
 *
 */
public class HpccFileTest {

  public static void main(String[] args) throws Exception {
    String MyVM = "127.0.0.1";
    String ML_Dev = "10.239.40.2";
    String test1Name = "~THOR::JDH::JAPI_TEST1";
    String test2Name = "~THOR::JDH::JAPI_TEST2";
    HpccFile hpcc = new HpccFile(test2Name, "http", ML_Dev, "8010", "", "");
    System.out.println("Getting file parts");
    FilePart[] parts = hpcc.getFileParts();
    for (int i=0; i<parts.length; i++) {
      System.out.println(parts[i].getFilename() + ":"
              + parts[i].getPrimaryIP()+ ":"
              + parts[i].getSecondaryIP() + ": "
              + parts[i].getThisPart());
    }
    RecordDef rd = hpcc.getRecordDefinition();
    FieldDef root_def = rd.getRootDef();
    Iterator<FieldDef> iter = root_def.getDefinitions();
    while (iter.hasNext()) {
      FieldDef field = iter.next();
      System.out.println(field.toString());
    }
    System.out.println("End test");
  }
}
