package org.hpccsystems.spark;

import java.io.Serializable;
import javax.xml.bind.DatatypeConverter;

/**
 * @author holtjd
 * A sequence of binary objects.
 */
public class BinarySeqContent extends Content implements Serializable {
  private static final long serialVersionUID = 1L;
  private byte[][] value;

  /**
   * Empty constructor for serialization.
   */
  protected BinarySeqContent() {
    this.value = new byte[0][];
  }

  /**
   * @param name field name
   * @param v content value
   */
  public BinarySeqContent(FieldType typ, String name, byte[][] v) {
    super(FieldType.SET_OF_BINARY, name);
    this.value = new byte[v.length][];
    for (int i=0; i<v.length; i++) {
      this.value[i] = new byte[v[i].length];
      for (int j=0; j<v[i].length; j++) this.value[i][j] = v[i][j];
    }
  }

  /**
   * @param def
   * @param v content value
   */
  public BinarySeqContent(FieldDef def, byte[][] v) {
    super(def);
    if (def.getFieldType() != FieldType.SET_OF_BINARY) {
      throw new IllegalArgumentException("Wrong field type in definition");
    }
    this.value = new byte[v.length][];
    for (int i=0; i<v.length; i++) {
      this.value[i] = new byte[v[i].length];
      for (int j=0; j<v[i].length; j++) this.value[i][j] = v[i][j];
    }
  }
  /**
   * The raw data content.
   * @return
   */
  public byte[][] asSetOfBinary() {
    byte[][] rslt = new byte[this.value.length][0];
    for (int i=0; i<this.value.length; i++) {
      rslt[i] = new byte[this.value[i].length];
      for (int j=0; j<this.value[i].length; j++) {
        rslt[i][j] = this.value[i][j];
      }
    }
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
    StringBuilder sb = new StringBuilder(100 + this.value.length*100);
    for (int i=0; i<this.value.length; i++) {
      if (i>0) sb.append(elementSep);
      sb.append(DatatypeConverter.printHexBinary(this.value[i]));
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
      rslt[i] = DatatypeConverter.printHexBinary(this.value[i]);
    }
    return rslt;
  }

}
