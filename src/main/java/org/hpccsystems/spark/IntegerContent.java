package org.hpccsystems.spark;

import static org.junit.Assert.assertNotNull;

public class IntegerContent extends Content {
  private final static long serialVersionUID = 1L;
  private long value;
  /**
   * Constructor for serialization
   */
  protected IntegerContent() {
    super();
    this.value = 0;
  }
  /**
   * Convenience constructor when no field def is available
   * @param name
   * @param value
   */
  public IntegerContent(String name, long v) {
    super(FieldType.INTEGER, name);
    this.value = v;
  }
  /**
   * Normal constructor
   * @param def the field definition
   * @param v the value
   */
  public IntegerContent(FieldDef def, long v) {
    super(def);
    if (def.getFieldType() != FieldType.INTEGER) {
      throw new IllegalArgumentException("Def must have Integer type");
    }
    this.value = v;
  }
  // access
  @Override
  public int numFields() {
    return 1;
  }
  @Override
  public long asInt() {
    return value;
  }
  @Override
  public long[] asSetOfInt() {
    long[] rslt = new long[1];
    rslt[0] = value;
    return rslt;
  }
  @Override
  public double asReal() {
    return (double) value;
  }
  @Override
  public double[] asSetOfReal() {
    double[] rslt = new double[1];
    rslt[0] = (double) value;
    return rslt;
  }
  @Override
  public String asString() {
    String rslt = Long.toString(this.value);
    return rslt;
  }
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = Long.toString(this.value);
    return rslt;
  }
  @Override
  public Content[] asRecord() {
    Content[] w = new Content[1];
    w[0] = this;
    return w;
  }
  @Override
  public RecordContent[] asSetOfRecord() {
    RecordContent[] rslt = new RecordContent[1];
    Content[] f = new Content[1];
    f[0] = this;
    rslt[0] = new RecordContent("Dummy", f);
    return rslt;
  }
  @Override
  public byte[] asBinary() {
    byte[] rslt = new byte[8];
    long work = this.value;
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
