package org.hpccsystems.spark;

import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.spark.temp.DFUFilePartsOnClusterInfo;
import org.hpccsystems.spark.temp.DFUFilePartInfo;
import org.hpccsystems.spark.temp.FilePartsFactory;
import org.hpccsystems.ws.client.platform.DFURecordDefInfo;
import org.hpccsystems.ws.client.platform.DFUDataColumnInfo;
import org.hpccsystems.ws.client.platform.EclRecordInfo;
import org.hpccsystems.ws.client.utils.Connection;

import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.Map;



/**
 * Test the build RDD for a distributed file on a THOR cluster.
 * @author John Holt
 *
 */
/**
 * @author John Holt
 *
 */
public class DFU_Files_tester {
  private HPCCWsDFUClient hpcc;

  /**
   * Builds a connection and then the DFU Client object
   * @param protocol http or https
   * @param targetHost IP or name
   * @param targetPort port number, usually 8010 or 18010 (HTTPS)
   * @param user user name
   * @param pword password
   */
  public DFU_Files_tester(String protocol, String targetHost, String targetPort,
         String user, String pword) {
    Connection conn = new Connection(protocol, targetHost, targetPort);
    conn.setUserName(user);
    conn.setPassword(pword);
    hpcc = HPCCWsDFUClient.get(conn);
  }

  /**
   * The DFU Ws Client object
   * @return the DFU client
   */
  public HPCCWsDFUClient getClient() {
    return hpcc;
  }

  public static void main(String[] args) throws Exception {
    //Files f_work = new DFU_Files("http", "127.0.0.1", "18010", "", "");
      DFU_Files_tester f_work = new DFU_Files_tester("http", "10.239.40.2", "8010", "", "");
    HPCCWsDFUClient hpcc = f_work.getClient();
    DFUFileDetailInfo fd = hpcc.getFileDetails("~THOR::JDH::JAPI_TEST", "");
    //DFUFileDetailInfo fd = hpcc.getFileDetails("~thor::persist::res1", "");
    //DFUFileDetailInfo fd = hpcc.getFileDetails("~thor::jdh::test_strings_2", "");
    //DFUFileDetailInfo fd = hpcc.getFileDetails("~thor::jdh::test::glass.csv", "");
    System.out.println("name: " + fd.getName());
    System.out.println("Filename: " + fd.getFilename());
    System.out.println("Prefix: " + fd.getPrefix());
    System.out.println("Directory: " + fd.getDir());
    System.out.println("Num parts: " + fd.getNumParts());
    System.out.println("File size: " + fd.getFilesize());
    System.out.println("Max Size: " + fd.getMaxRecordSize());
    System.out.println("Actual size: " + fd.getActualSize());
    System.out.println("Record size: " + fd.getRecordSize());
    System.out.println("Record count: " + fd.getRecordCount());
    System.out.println("Format: " + fd.getFormat());
    System.out.println("Ecl: " + fd.getEcl());
    System.out.println("Content type: " + fd.getContentType());
    DFUFilePartsOnClusterInfo[] fp = FilePartsFactory.makePartsOnCluster(fd);
    for (int f=0; fp!=null && f<fp.length; f++) {
      DFUFilePartInfo[] parts = fp[f].getDFUFileParts();
      System.out.println(parts.length);
      for (int i=0; i<parts.length; i++) {
        System.out.println(parts[i].getId() + ":"
                + parts[i].getCopy() + ":"
                + parts[i].getIp() + ": "
                + parts[i].getPartsize() + "; "
                + parts[i].getActualSize());
      }
    }
        System.out.println("Column definition information:");
        java.util.ArrayList<DFUDataColumnInfo> fields = fd.getColumns();
        for (DFUDataColumnInfo fld : fields) {
            System.out.println("$$$" + fld.toString());
            System.out.println();
        }
    System.out.println("Record definition information:");
    EclRecordInfo ri = fd.getRecordFromECL(fd.getEcl());
    HashMap<String, DFURecordDefInfo> rdef_map = ri.getRecordsets();
    for (Map.Entry<String, DFURecordDefInfo> entry : rdef_map.entrySet()) {
        System.out.println("---" + entry.getKey() + " ===> "
                + entry.getValue().toString());
    }
  }
}
