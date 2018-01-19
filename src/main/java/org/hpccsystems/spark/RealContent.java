package org.hpccsystems.spark;

import java.io.Serializable;

public class RealContent extends Content implements Serializable {
  private final static long serialVersionUID = 1L;
  private double value;
  /**
   * Empty constructor for serialization.
   */
  protected RealContent() {
    this.value = 0;
  }
  /**
   * Convenience constructor when FieldDef is not available
   * @param name the field name
   * @param v the value of the content
   */
  public RealContent(String name, double v) {
    super(FieldType.REAL, name);
    this.value = v;
  }
  /**
   * Normal constructor
   * @param def
   * @param v
   */
  public RealContent(FieldDef def, double v) {
    super(def);
    if (def.getFieldType()!=FieldType.REAL) {
      throw new IllegalArgumentException("Field definition has wrong type");
    }
    this.value = v;
  }
  @Override
  public int numFields() {
    return 1;
  }
  @Override
  public long asInt() {
    return (long) this.value;
  }
  @Override
  public long[] asSetOfInt() {
    long[] rslt = new long[1];
    rslt[0] = (long) this.value;
    return rslt;
  }
  @Override
  public double asReal() {
    return this.value;
  }
  @Override
  public double[] asSetOfReal() {
    double[] rslt = new double[1];
    rslt[0] = this.value;
    return rslt;
  }
  @Override
  public String asString() {
    String rslt = Double.toString(this.value);
    return rslt;
  }
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = Double.toString(this.value);
    return rslt;
  }
  @Override
  public Content[] asRecord() {
    Content[] rslt = new Content[1];
    rslt[0] = this;
    return rslt;
  }
  @Override
  public RecordContent[] asSetOfRecord() {
    RecordContent[] rslt = new RecordContent[1];
    Content[] w = new Content[1];
    w[0] = this;
    rslt[0] = new RecordContent("Dummy", w);
    return rslt;
  }
  @Override
  public byte[] asBinary() {
    byte[] rslt = new byte[8];
    long work = Double.doubleToLongBits(this.value);
    for (int i=0; i<8; i++ ) {
      rslt[7-i] = (byte)(work & ((long)0xff));
      work = work >> 8;
    }
    return rslt;
  }
  @Override
  public byte[][] asSetOfBinary() {
    byte[][] rslt = new byte[1][];
    rslt[0] = this.asBinary();
    return rslt;
  }

}
