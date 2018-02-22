package org.hpccsystems.spark;

import org.hpccsystems.spark.thor.UnusableDataDefinitionException;
import org.hpccsystems.spark.thor.ClusterRemapper;
import org.hpccsystems.spark.thor.RemapInfo;
import org.hpccsystems.ws.client.HPCCWsDFUClient;
import org.hpccsystems.ws.client.platform.DFUFileDetailInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartsOnClusterInfo;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;
import org.hpccsystems.ws.client.utils.Connection;

/**
 * Access to file content on a collection of one or more HPCC
 * clusters.
 * @author holtjd
 *
 */
public class HpccFile {
  private boolean ready;
  private boolean badFile;
  private boolean badDef;
  private String protocol;
  private String host;
  private String port;
  private String user;
  private String pword;
  private String fileName;
  private FilePart[] parts;
  private RecordDef recordDefinition;
  private RemapInfo ri;
  /**
   * Lazy constructor for the HpccFile.  Captures the information
   * but does not actually reach out the the DAFServer for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   */
  public HpccFile(String aFileName, String aProtocol, String aHost,
      String aPort, String aUser, String aPword) {
    this(aFileName, aProtocol, aHost, aPort, aUser, aPword,
         new RemapInfo(0));
  }
  /**
   * Lazy constructor for the HpccFile.  Captures the information
   * but does not actually reach out the the DAFServer for the
   * clusters behind the ESP named by the IP address.
   * @param fileName The HPCC file name
   * @param protocol usually http or https
   * @param host the ESP address
   * @param port the ESP port
   * @param user a valid account that has access to the file
   * @param pword a valid pass word for the account
   * @param remap_info address and port re-mapping info for THOR cluster
   */
  public HpccFile(String aFileName, String aProtocol, String aHost,
      String aPort, String aUser, String aPword, RemapInfo remap_info) {
    this.ready = false;
    this.badFile = false;
    this.badDef = false;
    this.fileName = aFileName;
    this.protocol = aProtocol;
    this.host = aHost;
    this.port = aPort;
    this.user = aUser;
    this.pword = aPword;
    this.parts = new FilePart[0];   // empty
    this.recordDefinition = new RecordDef();  // missing, the default
    this.ri = remap_info;
  }
  /**
   * The partitions for the file residing on an HPCC cluster
   * @return
   * @throws HpccFileException
   */
  public FilePart[] getFileParts() throws HpccFileException {
    if (this.badFile) {
      StringBuilder sb = new StringBuilder();
      sb.append("Failed to access file ");
      sb.append(this.fileName);
      throw new HpccFileException(sb.toString());
    }
    if (!this.ready) resolveFileInfo();
    FilePart[] rslt = new FilePart[parts.length];
    for (int i=0; i<parts.length; i++) rslt[i]=parts[i];
    return rslt;
  }
  /**
   * The record definition for a file on an HPCC cluster.
   * @return
   * @throws HpccFileException
   */
  public RecordDef getRecordDefinition() throws HpccFileException {
    if (this.badDef) throw new HpccFileException("Bad definition");
    if (!this.ready) resolveFileInfo();
    return recordDefinition;
  }
  private void resolveFileInfo() throws HpccFileException {
      this.ready = true;
      Connection conn = new Connection(this.protocol, this.host, this.port);
      conn.setUserName(user);
      conn.setPassword(pword);
      HPCCWsDFUClient hpcc = HPCCWsDFUClient.get(conn);
      try {
        DFUFileDetailInfo fd = hpcc.getFileDetails(this.fileName, "", true, false);
        DFUFilePartsOnClusterInfo[] fp = fd.getDFUFilePartsOnClusters();
        DFUFilePartInfo[] dfu_parts = fp[0].getDFUFileParts();
        ClusterRemapper cr = ClusterRemapper.makeMapper(this.ri, dfu_parts,
            fd.getNumParts());
        this.parts = FilePart.makeFileParts(fd.getNumParts(), fd.getDir(),
            fd.getFilename(), fd.getPathMask(), dfu_parts, cr);
        String record_def_json = fd.getJsonInfo();
        if (record_def_json==null) {
          throw new UnusableDataDefinitionException("Definiiton returned was null");
        }
        this.recordDefinition = RecordDef.parseJsonDef(record_def_json);
      } catch (UnusableDataDefinitionException e) {
        this.badDef = true;
        throw new HpccFileException(e);
      } catch (Exception e) {
        this.badFile = true;
        throw new HpccFileException(e);
      }
  }
}
