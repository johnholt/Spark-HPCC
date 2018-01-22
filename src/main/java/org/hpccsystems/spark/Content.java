package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * The field contents with the name and type of the data.  This is an
 * abstract type.  The implementation types are IntegerContent, RealContent,
 * BooleanContent, StringContent, RecordContent, IntegerSeqContent,
 * RealSeqContent, BooleanSeqContent, StringSeqContent, and
 * RecordSeqContent.
 * If the field is a composite type, the contents can be accessed by
 * field name.
 * Each content will supply alternative content.  For instance, an
 * IntegerContent object can supply a String value of the integer.
 * @author holtjd
 *
 */
public abstract class Content implements Serializable {
  static private final long serialVersionUID = 1L;
  private FieldType fieldType;
  private String fieldName;
  //
  protected Content() {
    this.fieldType = FieldType.MISSING;
    this.fieldName = "";
  }
  //
  /**
   * Convenience constructor
   * @param typ the type of the content
   * @param name field or column name of this content
   */
  public Content(FieldType typ, String name) {
    this.fieldType = typ;
    this.fieldName = name;
  }
  /**
   * Normal constructor
   * @param def the definition for this field
   */
  public Content(FieldDef def) {
    this.fieldType = def.getFieldType();
    this.fieldName = def.getFieldName();
  }
  /**
   * The name of this field
   * @return field name
   */
  public String getName() {  return this.fieldName; }
  /**
   * The type name of this field
   * @return type name
   */
  public String getTypeName() { return this.fieldType.name(); }
  /**
   * The type for this content.
   * @return field type enumeration value
   */
  public FieldType getFieldType() { return this.fieldType; }
  /**
   * Display the content with name.
   * @return a visual representation
   */
  public final String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("{");
    sb.append(this.fieldType.toString());
    sb.append(" field ");
    sb.append(this.fieldName);
    sb.append("=");
    sb.append(this.asString());
    sb.append("}");
    return sb.toString();
  }
  /**
   * The number of elements for this type.  One for scalars.
   * @return the number of elements, 1 for scalars and sets of scalars,
   * the number of fields for records and sets of records
   */
  public abstract int numFields();
  /**
   * The value of the content as a long integer.  Strings and Reals
   * will be converted.  If the content is a set (array) then the
   * first value is returned.
   * @return the content value as a single integer
   */
  public abstract long asInt();
  /**
   * The contents as a set of long integers.  String and
   * Real values are converted.
   * @return the array of long integers.
   */
  public abstract long[] asSetOfInt();
  /**
   * The content value as a real number.  Integers and Strings are
   * converted.  If the content is an array, then the first value
   * is returned.
   * @return the field value as a Real
   */
  public abstract double asReal();
  /**
   * The content values as a set of Reals.  Integers and Strings
   * are converted.
   * @return the array of Real values
   */
  public abstract double[] asSetOfReal();
  /**
   * The content value as a string.  Integers and Reals are converted
   * to strings.  If the content is an array, then the first value
   * is returned.
   * @return the field value as a String
   */
  public abstract String asString();
  /**
   * The content value as an array of Strings.  Integers and Reals are
   * converted.
   * @return the array of String values
   */
  public abstract String[] asSetOfString();
  /**
   * The field content of a record.  A single field record
   * is created if necessary.  This content type is either a file
   * record, or a sub-structure on a record.
   * @return the record
   */
  public abstract Content[] asRecord();
  /**
   * The field content is a set of records.  A file record
   * has a child dataset.
   * @return the dataset
   */
  public abstract RecordContent[] asSetOfRecord();
  /**
   * A binary string of the data
   * @return an array of byte values of the data
   */
  public abstract byte[] asBinary();
  /**
   * An array of byte arrays
   * @return an array of byte arrays
   */
  public abstract byte[][] asSetOfBinary();
}
