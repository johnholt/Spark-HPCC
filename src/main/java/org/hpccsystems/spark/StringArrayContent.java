/**
 *
 */
package org.hpccsystems.spark;

import java.text.NumberFormat;

/**
 * @author holtjd
 *
 */
public class StringArrayContent extends FieldContent {
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
  public Record asRecord() {
    FieldContent[] w = new FieldContent[1];
    w[0] = this;
    Record rslt = new Record(w, "Dummy", 0, 0);
    return rslt;
  }
  @Override
  public Record[] asSetOfRecord() {
    Record[] rslt = new Record[1];
    FieldContent[] f = new FieldContent[1];
    f[0] = this;
    rslt[0] = new Record(f, "Dummy", 0, 0);
    return rslt;
  }
}
