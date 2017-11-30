package org.hpccsystems.spark;

import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartsOnClusterInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.platform.DFURecordDefInfo;
import org.hpccsystems.ws.client.platform.DFUDataColumnInfo;
import org.hpccsystems.ws.client.platform.EclRecordInfo;
import org.hpccsystems.ws.client.utils.Connection;

import java.util.HashMap;
import java.util.Map;
import java.util.Iterator;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;



/**
 * Test the build RDD for a distributed file on a THOR cluster.
 * @author John Holt
 *
 */
/**
 * @author John Holt
 *
 */
public class DfuFilesTest {
  private HPCCWsDFUClient hpcc;

  /**
   * Builds a connection and then the DFU Client object
   * @param protocol http or https
   * @param targetHost IP or name
   * @param targetPort port number, usually 8010 or 18010 (HTTPS)
   * @param user user name
   * @param pword password
   */
  public DfuFilesTest(String protocol, String targetHost, String targetPort,
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
      DfuFilesTest f_work = new DfuFilesTest("http", "10.239.40.2", "8010", "", "");
    HPCCWsDFUClient hpcc = f_work.getClient();
    DFUFileDetailInfo fd = hpcc.getFileDetails("~THOR::JDH::JAPI_TEST1", "", true);
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
    System.out.println("Record size: " + fd.getRecordSize());
    System.out.println("Record count: " + fd.getRecordCount());
    System.out.println("Format: " + fd.getFormat());
    System.out.println("Ecl: " + fd.getEcl());
    System.out.println("Content type: " + fd.getContentType());
    DFUFilePartsOnClusterInfo[] fp = fd.getDFUFilePartsOnClusters();
    for (int f=0; fp!=null && f<fp.length; f++) {
      DFUFilePartInfo[] dfu_parts = fp[f].getDFUFileParts();
      System.out.println(dfu_parts.length);
      FilePart[] parts = FilePart.makeFileParts(fd.getNumParts(),
          fd.getDir(), fd.getFilename(), fd.getPathMask(), dfu_parts);
      for (int i=0; i<parts.length; i++) {
        System.out.println(parts[i].getFilename() + ":"
                + parts[i].getPrimaryIP()+ ":"
                + parts[i].getSecondaryIP() + ": "
                + parts[i].getThisPart());
      }
    }
    String record_def_json = fd.getJsonInfo();
    System.out.println("Record Structure in JSON");
    System.out.println(record_def_json);
    System.out.println("Working with JSON Objects");
    JsonFactory factory = new JsonFactory();
    JsonParser parse_obj = factory.createParser(record_def_json);
    JsonToken tok = parse_obj.nextValue();
    int obj_level = 0;
    while (tok != null) {
      if (tok==JsonToken.END_OBJECT) obj_level--;
      StringBuilder sb = new StringBuilder();
      for (int i=0; i<obj_level; i++) sb.append("  ");
      sb.append("Level ");
      sb.append(obj_level);
      sb.append(":  ");
      sb.append(tok.toString());
      sb.append(" ");
      sb.append(parse_obj.getCurrentName());
      sb.append("=");
      switch (tok) {
        case FIELD_NAME:
          sb.append(parse_obj.getCurrentName());
          break;
        case VALUE_NUMBER_INT:
          sb.append(parse_obj.getLongValue());
          break;
        case VALUE_NUMBER_FLOAT:
          sb.append(parse_obj.getDoubleValue());
          break;
        case VALUE_TRUE:
        case VALUE_FALSE:
          sb.append(parse_obj.getBooleanValue());
          break;
        case VALUE_STRING:
          sb.append('"');
          sb.append(parse_obj.getText());
          sb.append('"');
          break;
        default:
          sb.append('%');
          sb.append(parse_obj.getText());
          sb.append('%');
      }
      System.out.println(sb.toString());
      if (tok==JsonToken.START_OBJECT) obj_level++;
      tok = parse_obj.nextValue();
    }
    System.out.println("End parse");
  }
}
