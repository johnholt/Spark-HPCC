/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * @author holtjd
 *
 */
public abstract class FieldContent implements Serializable {
  static private final long serialVersionUID = 1L;
  private FieldType fieldType;
  private String fieldName;
  //
  protected FieldContent() {
    this.fieldType = FieldType.MISSING;
    this.fieldName = "";
  }
  //
  /**
   * Convenience constructor
   * @param typ the type of the content
   * @param name field or column name of this content
   */
  public FieldContent(FieldType typ, String name) {
    this.fieldType = typ;
    this.fieldName = name;
  }
  /**
   * Normal constructor
   * @param def the definition for this field
   */
  public FieldContent(FieldDef def) {
    this.fieldType = def.getFieldType();
    this.fieldName = def.getFieldName();
  }
  public String getName() {  return fieldName; }
  public String getTypeName() { return fieldType.name(); }
  //
  public abstract byte[] asData();
  public byte[][] asSetOfData() {
    byte[][] rslt = new byte[1][];
    rslt[0] = this.asData();
    return rslt;
  }
  public abstract long asInt();
  public long[] asSetOfInt() {
    long[]rslt = new long[1];
    rslt[0] = this.asInt();
    return rslt;
  }
  public abstract double asReal();
  public double[] asSetOfReal() {
    double[] rslt = new double[1];
    rslt[0] = this.asReal();
    return rslt;
  }
  public abstract String asString();
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = this.asString();
    return rslt;
  }
  public Record asRecord() {
    FieldContent[] w = new FieldContent[1];
    w[0] = this;
    Record rslt = new Record(w);
    return rslt;
  }
  public Record[] asSetOfRecord() {
    Record[] rslt = new Record[1];
    FieldContent[] f = new FieldContent[1];
    f[0] = this;
    rslt[0] = new Record(f);
    return rslt;
  }
}
