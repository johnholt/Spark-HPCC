/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;
import java.text.NumberFormat;
import java.text.ParseException;
import java.util.Comparator;
import java.util.Arrays;
import org.apache.spark.Partition;
import org.hpccsystems.ws.client.platform.DFUFilePartInfo;

/**
 * @author holtjd
 *
 */
public class FilePart implements Partition, Serializable {
  static private final long serialVersionUID = 1L;
  static private NumberFormat fmt = NumberFormat.getInstance();
  private String primary_ip;
  private String secondary_ip;
  private String file_name;
  private int this_part;
  private int num_parts;
  private long part_size;

  private FilePart(String ip0, String ipx, String dir, String name,
      int this_part, int num_parts, long part_size, String mask) {
    String p_str = Integer.toString(this_part);
    String f_str = dir + "/" + mask;
    this.primary_ip = ip0;
    this.secondary_ip = ipx;
    this.file_name = f_str.replace("$P$", p_str);
    this.this_part = this_part;
    this.num_parts = num_parts;
    this.part_size = part_size;
  }
  private FilePart() {}

  public String getPrimaryIP() { return this.primary_ip; }
  public String getSecondaryIP() { return this.secondary_ip; }
  public String getFilename() {
    return this.file_name;
  }
  public int getThisPart() { return this.this_part; }
  public int getNumParts() { return this.num_parts; }
  public long getPartSize() { return this.part_size; }

  /* (non-Javadoc)
   * @see org.apache.spark.Partition#index()
   */
  public int index() {
    return this_part - 1;
  }
  /* (non-Javadoc)
   * Spark core 2.10 needs this defined, not needed in 2.11
   */
  public boolean org$apache$spark$Partition$$super$equals(Object arg0) {
    if (!(arg0 instanceof FilePart)) return false;
    FilePart fp0 = (FilePart) arg0;
    if (!this.getFilename().equals(fp0.getFilename())) return false;
    if (this.getNumParts() != fp0.getNumParts()) return false;
    if (this.getThisPart() != fp0.getThisPart()) return false;
    if (!this.getPrimaryIP().equals(fp0.getPrimaryIP())) return false;
    if (!this.getSecondaryIP().equals(fp0.getSecondaryIP())) return false;
    return true;
  }
  /**
   * Create an array of Spark partition objects for HPCC file parts.
   * @param num_parts the number of parts for the file
   * @param dir the directory name for the file
   * @param name the base name of the file
   * @param mask the mask for the file name file part suffix
   * @param parts an array of JAPI file part info objects
   * @return an array of partitions for Spark
   */
  public static FilePart[] makeFileParts(int num_parts, String dir,
      String name, String mask, DFUFilePartInfo[] parts) {
    FilePart[] rslt = new FilePart[num_parts];
    Arrays.sort(parts, FilePartInfoComparator);
    int copies = parts.length / num_parts;
    int posSecondary = (copies==1) ? 0 : 1;
    for (int i=0; i<num_parts; i++) {
      DFUFilePartInfo primary = parts[i * copies];
      DFUFilePartInfo secondary = parts[(i * copies) + posSecondary];
      int partSize;
      try {
        partSize = (primary.getPartsize()!="")
            ? fmt.parse(primary.getPartsize()).intValue()  : 0;
      } catch (ParseException e) {
        partSize = 0;
      }
      rslt[i] = new FilePart(primary.getIp(), secondary.getIp(),
          dir, name, i+1, num_parts, partSize, mask);
    }
    return rslt;
  }
  public static Comparator<DFUFilePartInfo> FilePartInfoComparator
                = new Comparator<DFUFilePartInfo>() {
    public int compare(DFUFilePartInfo fpi1, DFUFilePartInfo fpi2) {
      if (fpi1.getId() < fpi2.getId()) return -1;
      if (fpi1.getId() > fpi2.getId()) return 1;
      if (fpi1.getCopy() < fpi2.getCopy()) return -1;
      if (fpi1.getCopy() > fpi2.getCopy()) return 1;
      return 0;
    }
  };

}
