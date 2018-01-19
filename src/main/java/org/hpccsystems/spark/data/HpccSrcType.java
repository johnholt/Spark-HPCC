package org.hpccsystems.spark.data;

/**
 * @author holtjd
 * The type of the binary source data.
 */
public enum HpccSrcType {
  SINGLE_BYTE_CHAR("Single Byte Charset"),
  UTF8("Unicode UTF8"),
  UTF16BE("Unicode UTF16 big endian"),
  UTF16LE("Unicode UTF16 little endian"),
  BIG_ENDIAN("big endian"),
  LITTLE_ENDIAN("little endian"),
  BINARY_CODED_DECIMAL("Binary coded decimal"),
  UNKNOWN("Unkown");
  //
  private String description;
  //
  HpccSrcType(String desc) {
    this.description = desc;
  }
  /**
   * Description of the enumeration value;
   * @return a description
   */
  public String getDescription() { return description; }
}
