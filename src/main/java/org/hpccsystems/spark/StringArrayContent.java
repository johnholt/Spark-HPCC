/**
 *
 */
package org.hpccsystems.spark;

import java.text.NumberFormat;

/**
 * @author holtjd
 *
 */
public class StringArrayContent extends Content {
  private static final long serialVersionUID = 1L;
  static private NumberFormat fmt = NumberFormat.getInstance();
  private String[] values;
  /**
   * Empty constructor for serialization
   */
  protected StringArrayContent() {
    this.values = new String[0];
  }
  /**
   * Constructor without a field def, copies content array
   * @param name the name of the field
   * @param content the array of strings
   */
  public StringArrayContent(String name, String[] content) {
    super(FieldType.SET_OF_STRING, name);
    this.values = new String[content.length];
    for (int i=0; i<content.length; i++) this.values[i]=content[i];
  }
  /**
   * Normal public constructor.  Copies the array.
   * @param def the field definition
   * @param content
   */
  public StringArrayContent(FieldDef def, String[] content) {
    super(def);
    this.values = new String[content.length];
    for (int i=0; i<content.length; i++) this.values[i] = content[i];
  }
  @Override
  public int numFields() {
    return 1; // only 1 field!
  }
  @Override
  public long asInt() {
    long rslt = 0;
    try {
      rslt = fmt.parse(this.values[0]).longValue();
    } catch (Exception e) {
      rslt = 0;
    }
    return rslt;
  }
  @Override
  public long[] asSetOfInt() {
    long[] rslt = new long[this.values.length];
    for (int i=0; i<this.values.length; i++) {
      try {
        rslt[i] = fmt.parse(this.values[i]).longValue();
      } catch (Exception e) {
        rslt[i] = 0;
      }
    }
    return rslt;
  }
  @Override
  public double asReal() {
    double rslt = 0.0;
    try {
      rslt = fmt.parse(this.values[0]).doubleValue();
    } catch (Exception e) {
      rslt = 0;
    }
    return rslt;
  }
  @Override
  public double[] asSetOfReal() {
    double[] rslt = new double[this.values.length];
    for (int i=0; i<this.values.length; i++) {
      try {
        rslt[i] = fmt.parse(this.values[i]).doubleValue();
      } catch (Exception e) {
        rslt[i] = 0.0;
      }
    }
    return rslt;
  }
  @Override
  public String asString() {
    return this.values[0];
  }
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.values.length];
    for (int i=0; i<this.values.length; i++) rslt[i] = this.values[i];
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
    char[] chars = this.values[0].toCharArray();
    byte[] rslt = new byte[chars.length*Character.BYTES];
    for (int i=0; i<chars.length; i++ ) {
      int high = i*2;
      int low = high + 1;
      rslt[high] = 0;
      rslt[low] = 0;
    }
    return rslt;
  }
  @Override
  public byte[][] asSetOfBinary() {
    byte[][] rslt = new byte[this.values.length][];
    for (int i=0; i<this.values.length; i++) {
      char[] chars = this.values[i].toCharArray();
      rslt[i] = new byte[chars.length*Character.BYTES];
      for (int j=0; j<chars.length; j++) {
        int high = j*2;
        int low = high + 1;
        rslt[i][high] = (byte)((chars[j] & 0xff00) >> 8);
        rslt[i][low] = (byte)(chars[i] & 0x00ff);
      }
    }
    return rslt;
  }
}
