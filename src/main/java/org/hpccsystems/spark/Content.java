package org.hpccsystems.spark;

import java.io.Serializable;

import org.hpccsystems.spark.thor.FieldDef;

/**
 * The field contents with the name and type of the data.  This is an
 * abstract type.  The implementation types are IntegerContent, RealContent,
 * BooleanContent, StringContent, BinaryContent, RecordContent, IntegerSeqContent,
 * RealSeqContent, BooleanSeqContent, StringSeqContent, BinarySeqContent, and
 * RecordSeqContent.
 *
 * Each instance type will use the corresponding Java primitive type wrappers
 * to create a string version for the asString and asStringArray methods.
 *
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
   * The content value as a string.  Integers and Reals are converted
   * to strings.  If the content is an array, then the values are separated
   * by the arraySep value.  If the content is a record, then the values
   * are separated by the fieldSep value.
   * @param fieldSep a field separation string used when the type
   * is an array or sequence
   * @param elementSep an array element separation string used when there are
   * more than one value held in an array or sequence
   * @return the field value as a String
   */
  public abstract String asString(String fieldSep, String elementSep);
  /**
   * The content value as a string.  Fields are separated by a comma and space
   * and array entries are separated by a semicolon and space.
   * @return a string
   */
  public String asString() {
    return this.asString(", ", "; ");
  }
  /**
   * The content value as an array of Strings.  Integers and Reals are
   * converted to strings.
   * @return the array of String values
   */
  public abstract String[] asSetOfString();
}
