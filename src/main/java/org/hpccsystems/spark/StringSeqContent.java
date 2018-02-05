/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * @author holtjd
 *
 */
public class StringSeqContent extends Content implements Serializable{
  private static final long serialVersionUID = 1L;
  private boolean isAll;
  private String[] values;
  /**
   * Empty constructor for serialization
   */
  protected StringSeqContent() {
    this.values = new String[0];
    this.isAll = false;
  }
  /**
   * Constructor without a field def, copies content array
   * @param name the name of the field
   * @param content the array of strings
   * @param f Universal set, all values
   */
  public StringSeqContent(String name, String[] content, boolean f) {
    super(FieldType.SET_OF_STRING, name);
    this.values = new String[content.length];
    for (int i=0; i<content.length; i++) this.values[i]=content[i];
    this.isAll = f;
  }
  /**
   * Normal public constructor.  Copies the array.
   * @param def the field definition
   * @param content
   * @param f Universal set, all values
   */
  public StringSeqContent(FieldDef def, String[] content, boolean f) {
    super(def);
    this.values = new String[content.length];
    for (int i=0; i<content.length; i++) this.values[i] = content[i];
    this.isAll = f;
  }
  /**
   * Is this a universe of values
   * @return
   */
  public boolean isAllValues() { return isAll; }

  @Override
  public int numFields() {
    return 1; // only 1 field!
  }
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<this.values.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(this.values[i]);
    }
    return sb.toString();
  }
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[this.values.length];
    for (int i=0; i<this.values.length; i++) rslt[i] = this.values[i];
    return rslt;
  }

}
