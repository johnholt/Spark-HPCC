package org.hpccsystems.spark;

import java.io.Serializable;

import org.hpccsystems.spark.thor.FieldDef;

/**
 * @author holtjd
 * A sequence of boolean values
 */
public class BooleanSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private boolean isAll;
  private boolean[] value;
  /**
   * Empty constructor for serializations
   */
  public BooleanSeqContent() {
    this.value = new boolean[0];
    this.isAll = false;
  }

  /**
   * @param typ
   * @param name
   * @param v the content values
   * @param f Universal set, all values
   */
  public BooleanSeqContent(String name, boolean[] v, boolean f) {
    super(FieldType.SET_OF_BOOLEAN, name);
    this.value = new boolean[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
    this.isAll = f;
  }

  /**
   * @param def
   * @param v the set of values
   * @param f Universal set, all values
   */
  public BooleanSeqContent(FieldDef def, boolean[] v, boolean f) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_BOOLEAN) {
      throw new IllegalArgumentException("Definition has wrong type");
    }
    this.isAll = f;
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
  /**
   * Is this a universe set
   * @return
   */
  public boolean isAllValues() { return this.isAll; }

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
