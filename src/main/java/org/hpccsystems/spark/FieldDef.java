package org.hpccsystems.spark;

import org.hpccsystems.spark.FieldType;
import java.io.Serializable;

/**
 * The name and field type for an item from the HPCC environment.  The
 * types may be single scalar types or may be arrays or structures.
 *
 * @author holtjd
 *
 */

public class FieldDef implements Serializable {
  static final long serialVersionUID = 1L;
  private String fieldName;
  private FieldType fieldType;
  //
  protected FieldDef() {
    this.fieldName = "";
    this.fieldType = FieldType.MISSING;
  }
  /**
   * @param fieldName the name for the field or set or structure
   * @param fieldType the type
   */
  public FieldDef(String fieldName, FieldType fieldType) {
      this.fieldName = fieldName;
      this.fieldType = fieldType;
  }
  public String getFieldName() {
    return fieldName;
  }
  public FieldType getFieldType() {
    return fieldType;
  }
  @Override
  public String toString() {
    return "FieldDef [fieldName=" + fieldName + ", fieldType="
          + fieldType.description() + "]";
  }
  /**
   * The type name based upon the field type enum.
   *
   */
  public String typeName() {
    String rslt;
    switch (fieldType) {
      case BINARY:
        rslt = "BINARY";
        break;
      case INTEGER :
        rslt = "INTEGER";
        break;
      case REAL :
        rslt = "REAL";
        break;
      case STRING :
        rslt = "STRING";
        break;
      case MISSING :
        rslt = "ANY";
        break;
      default:
        rslt = "Unknown";
    }
    return rslt;
  }
  /**
   * Record name if this is a composite field
   * @return a blank name.
   */
  public String recordName() { return ""; }
}
