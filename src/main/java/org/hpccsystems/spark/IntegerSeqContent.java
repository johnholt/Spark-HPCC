package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * @author holtjd
 * Content for SET OF INTEGER or SET OF UNSIGNED
 */
public class IntegerSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private long[] value;
  /**
   * Empty constructor for serialization support
   */
  public IntegerSeqContent() {
    this.value = new long[0];
  }

  /**
   * @param name
   * @param v content values
   */
  public IntegerSeqContent(String name, long[] v) {
    super(FieldType.SET_OF_INTEGER, name);
    this.value = new long[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }

  /**
   * @param def
   * @param v content values
   */
  public IntegerSeqContent(FieldDef def, long[] v) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_INTEGER) {
      throw new IllegalArgumentException("Incorrect field type");
    }
    this.value = new long[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }
  /**
   * Content in raw form as a set of long integers
   * @return the content
   */
  public long[] asSetOfInt() {
    long[] rslt = new long[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = this.value[i];
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    return 1;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString()
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder(10 + this.value.length*10);
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(Long.toString(this.value[i]));
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.value.length];
    for (int i=0; i<this.value.length; i++) rslt[i] = Long.toString(this.value[i]);
    return rslt;
  }

}
