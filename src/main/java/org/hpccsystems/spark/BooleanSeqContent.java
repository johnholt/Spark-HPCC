package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * @author holtjd
 * A sequence of boolean values
 */
public class BooleanSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean[] value;
  /**
   * Empty constructor for serializations
   */
  public BooleanSeqContent() {
    this.value = new boolean[0];
  }

  /**
   * @param typ
   * @param name
   * @param v the content values
   */
  public BooleanSeqContent(String name, boolean[] v) {
    super(FieldType.SET_OF_BOOLEAN, name);
    this.value = new boolean[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }

  /**
   * @param def
   * @param v the set of values
   */
  public BooleanSeqContent(FieldDef def, boolean[] v) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_BOOLEAN) {
      throw new IllegalArgumentException("Definition has wrong type");
    }
  }
  /**
   * The content in raw form
   * @return the set of booleans
   */
  public boolean[] asSetOfBool() {
    boolean[] rslt = new boolean[this.value.length];
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
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(Boolean.toString(this.value[i]));
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.value.length];
    for (int i=0; i<this.value.length; i++) {
      rslt[i] = Boolean.toString(this.value[i]);
    }
    return rslt;
  }

}
