/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;
import java.util.Comparator;
import java.util.Arrays;
import org.apache.spark.Partition;
import org.hpccsystems.spark.temp.DFUFilePartInfo;

/**
 * @author holtjd
 *
 */
public class FilePart implements Partition, Serializable {
  static private final long serialVersionUID = 1L;
  private String primary_ip;
  private String secondary_ip;
  private String base_name;
  private int this_part;
  private int num_parts;
  private long part_size;

  private FilePart(String ip0, String ipx, String name,
      int this_part, int num_parts, long part_size) {
    this.primary_ip = ip0;
    this.secondary_ip = ipx;
    this.base_name = name;
    this.this_part = this_part;
    this.num_parts = num_parts;
    this.part_size = part_size;
  }
  private FilePart() {}

  public String getPrimaryIP() { return this.primary_ip; }
  public String getSecondaryIP() { return this.secondary_ip; }
  public String getFilename() {
    return this.base_name;
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
  /**
   * Create an array of Spark partition objects for HPCC file parts.
   * @param num_parts the number of parts for the file
   * @param base_name the base name of the file
   * @param parts an array of JAPI file part info objects
   * @return an array of partitions for Spark
   */
  public static FilePart[] makeFileParts(int num_parts, String base_name,
      DFUFilePartInfo[] parts) {
    FilePart[] rslt = new FilePart[num_parts];
    Arrays.sort(parts, FilePartInfoComparator);
    int copies = parts.length / num_parts;
    int posSecondary = (copies==1) ? 0 : 1;
    for (int i=0; i<num_parts; i++) {
      DFUFilePartInfo primary = parts[i * copies];
      DFUFilePartInfo secondary = parts[(i * copies) + posSecondary];
      int partSize = (primary.getPartsize()!="")
          ? Integer.parseInt(primary.getPartsize())  : 0;
      rslt[i] = new FilePart(primary.getIp(), secondary.getIp(),
          base_name, i+1, num_parts, partSize);
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
