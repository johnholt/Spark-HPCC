package org.hpccsystems.spark;


public class IntegerContent extends FieldContent {
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
  public Record asRecord() {
    FieldContent[] w = new FieldContent[1];
    w[0] = this;
    Record rslt = new Record(w);
    return rslt;
  }
  @Override
  public Record[] asSetOfRecord() {
    Record[] rslt = new Record[1];
    FieldContent[] f = new FieldContent[1];
    f[0] = this;
    rslt[0] = new Record(f);
    return rslt;
  }

}
