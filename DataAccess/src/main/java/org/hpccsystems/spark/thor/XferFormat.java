package org.hpccsystems.spark.thor;

import org.hpccsystems.spark.RecordDef;

/**
 * @author holtjd
 * Transfer type enumeration
 */
public enum XferFormat {
  JSON("JSON"),
  XML("XML"),
  BINARY("Binary");
  //
  private String description;
  //
  XferFormat(String str) {
    this.description = str;
  }
  /**
   * The transfer format description text.
   * @return description
   */
  public String getDescription() { return this.description; }
  /**
   * The best request ofrmat to use for this record definition.
   * @param rd
   * @return The enum for the best type to use to transfer the data;
   */
  public XferFormat bestFormat(RecordDef rd) {
    return BINARY;
  }
}
