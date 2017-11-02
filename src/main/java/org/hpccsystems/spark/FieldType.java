/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * The data types for data fields on an HPCC record.
 *
 * @author holtjd
 *
 */
public enum FieldType implements Serializable {
  INTEGER(true, "Integer", false),
  REAL(true, "Real", false),
  STRING(true, "String", false),
  BINARY(true, "Binary Data", false),
  RECORD(false, "Record", true),
  MISSING(true, "Missing value", false),
  SET_OF_INTEGER(false, "Set of integers", false),
  SET_OF_REAL(false, "Set of reals", false),
  SET_OF_STRING(false, "Set of strings", false),
  SET_OF_BINARY(false, "Set of binary data", false),
  SET_OF_RECORD(false, "Set of records", true);

  static final long serialVersionUID = 1L;
  private boolean scalar;
  private String name;
  private boolean composite;
  FieldType(boolean atomicType, String name, boolean composite) {
    this.scalar = atomicType;
    this.name = name;
    this.composite = composite;
  }
  FieldType() {
    this.scalar = true;
    this.name = "";
    this.composite = false;
  }
  public boolean isScalar() { return this.scalar; }
  public boolean isVector() { return !this.scalar; }
  public boolean isComposite() { return this.composite; }
  public String description() {
    return name;
  }
}
