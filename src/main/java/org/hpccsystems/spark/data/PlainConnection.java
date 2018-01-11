/**
 *
 */
package org.hpccsystems.spark.data;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import org.hpccsystems.spark.FilePart;
import org.hpccsystems.spark.RecordDef;
import org.hpccsystems.spark.HpccFileException;

/**
 * @author holtjd
 * The connection to a specific THOR node for a specific file part.
 *
 */
public class PlainConnection {
  private boolean active;
  private boolean closed;
  private byte[] cursorBin;
  private int handle;
  private FilePart filePart;
  private RecordDef recDef;
  private int port;
  private java.io.DataInputStream dis;
  private java.io.DataOutputStream dos;
  private java.net.Socket sock;
  //
  private static final Charset hpccSet = Charset.forName("ISO-8859-1");
  private static final byte[] hyphen = "-".getBytes(hpccSet);
  private static final byte[] uc_J = "J".getBytes(hpccSet);
  /**
   * A plain socket connect to a THOR node for remote read
   * @param filePart the remote file name and IP
   * @param rd the JSON definition for the read input and output
   */
  public PlainConnection(FilePart fp, RecordDef rd) {
    this.port = 7100;
    this.recDef = rd;
    this.filePart = fp;
    this.active = false;
    this.closed = false;
    this.handle = 0;
    this.cursorBin = new byte[0];
  }
  /**
   * The remote file name.
   * @return file name
   */
  public String getFilename() { return filePart.getFilename(); }
  /**
   * The primary IP for the file part
   * @return IP address
   */
  public String getIP() { return filePart.getPrimaryIP(); }
  /**
   * The port number for the remote read service
   * @return port number
   */
  public int getPort() { return port; }
  /**
   * The read transaction in JSON format
   * @return read transaction
   */
  public String getTrans() { return makeRequest(); }
  /**
   * Is the read active?
   */
  public boolean isActive() { return this.active; }
  /**
   * Is the remote file closed?  The file is closed after
   * all of the partition content has been transferred.
   * @return true if closed.
   */
  public boolean isClosed() { return this.closed; }
  /**
   * Remote read handle for next read
   * @return the handle
   */
  public int getHandle() { return handle; }
  /**
   * Read a block of the remote file from a THOR node
   * @return the block sent by the node
   * @throws HpccFileException a problem with the read operation
   */
  public byte[] readBlock()
    throws HpccFileException {
    byte[] rslt = new byte[0];
    if (this.closed) return rslt;  // no data left to send
    if (!this.active) makeActive();
    try {
      Charset charset = Charset.forName("ISO-8859-1");
      String readTrans = makeRequest();
      int transLen = readTrans.length();
      dos.writeInt(transLen);
      dos.write(readTrans.getBytes(charset),0,transLen);
      dos.flush();
    } catch (IOException e) {
      throw new HpccFileException("Failed on remote read read trans", e);
    }
    int len = 0;
    boolean hi_flag = false;
    try {
      len = dis.readInt();
      if (len < 0) {
        hi_flag = true;
        len &= 0x7FFFFFFF;
      } else if (len==0) {
        this.closed = true;
        return rslt;
      }
      byte flag = dis.readByte();
      if (flag==hyphen[0]) {
        int msgLen = dis.readInt();
        byte[] msg = new byte[msgLen];
        this.dis.read(msg);
        String message = new String(msg, hpccSet);
        throw new HpccFileException("Failed with " + message);
      }
      if (flag != uc_J[0]) {
        StringBuilder sb = new StringBuilder();
        sb.append("Invalid response of ");
        sb.append(String.format("%02X ", flag));
        sb.append("received from THOR node ");
        sb.append(this.getIP());
        throw new HpccFileException(sb.toString());
      }
      len--;  // account for flag byte read
      this.handle = dis.readInt();
      // need to deal with 0 handle -> unknown needs to read again
      int dataLen = dis.readInt();
      if (dataLen == 0) {
        closeConnection();
        return rslt;
      }
      rslt = new byte[dataLen];
      for (int i=0; i<dataLen; i++) rslt[i] = dis.readByte();
      int cursorLen = dis.readInt();
      if (cursorLen == 0) {
        closeConnection();
        return rslt;
      }
      this.cursorBin = new byte[cursorLen];
      for (int i=0; i<cursorLen; i++) this.cursorBin[i] = dis.readByte();
    } catch (IOException e) {
      throw new HpccFileException("Error during read block", e);
    }
    return rslt;
  }
  /**
   * Open client socket to the primary and open the streams
   * @throws HpccFileException
   */
  private void makeActive() throws HpccFileException{
    this.active = false;
    this.handle = 0;
    this.cursorBin = new byte[0];
    try {
      sock = new java.net.Socket(this.getIP(), port);
    } catch (java.net.UnknownHostException e) {
      throw new HpccFileException("Bad file part addr "+this.getIP(), e);
    } catch (java.io.IOException e) {
      throw new HpccFileException(e);
    }
    try {
      dos = new java.io.DataOutputStream(sock.getOutputStream());
      dis = new java.io.DataInputStream(sock.getInputStream());
    } catch (java.io.IOException e) {
      throw new HpccFileException("Failed to create streams", e);
    }
    this.active = true;
  }
  /**
   * Creates a request string using the record definition, filename,
   * and current state of the file transfer.
   * @return request string
   */
  private String makeRequest() {
    StringBuilder sb = new StringBuilder(50
        + this.filePart.getFilename().length()
        + this.recDef.getJsonInputDef().length()
        + this.recDef.getJsonOutputDef().length());
    sb.append("{ \"format\" : \"binary\", \"node\" : ");
    sb.append("{\n \"kind\" : \"diskread\",\n \"fileName\" : \"");
    sb.append(this.filePart.getFilename());
    sb.append("\",\n \"input\" : ");
    sb.append(this.recDef.getJsonInputDef());
    sb.append(", \n \"output\" : ");
    sb.append(this.recDef.getJsonOutputDef());
    sb.append("\n }  }\n\n");
    return sb.toString();
  }
  private void closeConnection() throws HpccFileException {
    this.closed = true;
    try {
      dos.close();
      dis.close();
      sock.close();
    } catch (IOException e) {}  // ignore this
    this.dos = null;
    this.dis = null;
    this.sock = null;
  }
}
