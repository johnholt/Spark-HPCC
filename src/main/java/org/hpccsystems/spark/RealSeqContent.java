package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * @author holtjd
 * A set or array of real values.
 */
public class RealSeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private double[] value;

  /**
   * Empty constructor for serialization
   */
  protected RealSeqContent() {
    this.value = new double[0];
  }

  /**
   * @param name the field name
   * @param v the value for this content item
   */
  public RealSeqContent(String name, double[] v) {
    super(FieldType.SET_OF_REAL, name);
    this.value = new double[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }

  /**
   * @param def
   */
  public RealSeqContent(FieldDef def, double[] v) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_REAL) {
      throw new IllegalArgumentException("Incorrect type for field definition");
    }
    this.value = new double[v.length];
    for (int i=0; i<v.length; i++) this.value[i] = v[i];
  }
  /**
   * The content value in raw form
   * @return
   */
  public double[] asSetofReal() {
    double[] rslt = new double[this.value.length];
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
   * @see org.hpccsystems.spark.Content#asString(java.lang.String, java.lang.String)
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder(20 + this.value.length*10);
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(Double.toString(this.value[i]));
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
        rslt[i] = Double.toString(this.value[i]);
    }
    return rslt;
  }

}
