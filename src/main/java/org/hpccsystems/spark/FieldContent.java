/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * The field contents include the name and type of the data.
 * @author holtjd
 *
 */
public abstract class FieldContent implements Serializable {
  static private final long serialVersionUID = 1L;
  private FieldType fieldType;
  private String fieldName;
  //
  protected FieldContent() {
    this.fieldType = FieldType.MISSING;
    this.fieldName = "";
  }
  //
  /**
   * Convenience constructor
   * @param typ the type of the content
   * @param name field or column name of this content
   */
  public FieldContent(FieldType typ, String name) {
    this.fieldType = typ;
    this.fieldName = name;
  }
  /**
   * Normal constructor
   * @param def the definition for this field
   */
  public FieldContent(FieldDef def) {
    this.fieldType = def.getFieldType();
    this.fieldName = def.getFieldName();
  }
  /**
   * The name of this field
   * @return field name
   */
  public String getName() {  return fieldName; }
  /**
   * The type name of this field
   * @return type name
   */
  public String getTypeName() { return fieldType.name(); }
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
   * The field content as a record.  A single field record
   * is created if necessary.  This content type is either a file
   * record, or a sub-structure on a record.
   * @return the record
   */
  public abstract Record asRecord();
  /**
   * The field content is a set of records.  A file record
   * has a child dataset.
   * @return the dataset
   */
  public abstract Record[] asSetOfRecord();
}
