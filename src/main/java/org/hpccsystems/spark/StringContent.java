package org.hpccsystems.spark;

import java.text.NumberFormat;

/**
 * A field content item with String as the native type.
 * @author holtjd
 *
 */
public class StringContent extends Content {
  private static final long serialVersionUID = 1L;
  static private NumberFormat fmt = NumberFormat.getInstance();
  private String value;
  /**
   * No argument constructor for serialization
   */
  protected StringContent() {
    super();
    this.value = "";
  }
  /**
   * Construct a StringContent item without a FieldDef
   * @param v the string value
   * @param name the name of the field
   */
  public StringContent(String name, String v) {
    super(FieldType.STRING, name);
    this.value = v;
  }
  /**
   * Construct a StringContent item in the normal manner
   * @param def the FieldDef for this field
   * @param v the value of the field
   */
  public StringContent(FieldDef def, String v) {
    super(def);
    if (def.getFieldType() != FieldType.STRING) {
      throw new IllegalArgumentException("def must be String type");
    }
    this.value = v;
  }
  /*
   * (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#numFields()
   */
  @Override
  public int numFields() { return 1; }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asInt()
   */
  @Override
  public long asInt() {
    long rslt = 0;
    try {
      rslt = fmt.parse(this.value).longValue();
    } catch (Exception e) {
      rslt = 0;
    }
    return rslt;
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asReal()
   */
  @Override
  public double asReal() {
    double rslt = 0.0;
    try {
      rslt = fmt.parse(this.value).doubleValue();
    } catch (Exception e) {
      rslt = 0;
    }
    return rslt;
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asString()
   */
  @Override
  public String asString() {
    return this.value;
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asSetOfInt()
   */
  @Override
  public long[] asSetOfInt() {
    long[]rslt = new long[1];
    rslt[0] = this.asInt();
    return rslt;
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asSetOfReal()
   */
  @Override
  public double[] asSetOfReal() {
    double[] rslt = new double[1];
    rslt[0] = this.asReal();
    return rslt;
  }
  /* (non-javadoc)
   * @see org.hpccsystems.spark.FieldContent#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = this.asString();
    return rslt;
  }
  /*
   * (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asRecord()
   */
  @Override
  public Content[] asRecord() {
    Content[] w = new Content[1];
    w[0] = this;
    return w;
  }
  /*
   * (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asSetOfRecord()
   */
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
    char[] chars = this.value.toCharArray();
    byte[] rslt = new byte[chars.length*Character.BYTES];
    for (int i=0; i<chars.length; i++ ) {
      int high = i*2;
      int low = high + 1;
      rslt[high] = (byte)((chars[i] & 0xff00) >> 8);
      rslt[low] = (byte)(chars[i] & 0x00ff);
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
