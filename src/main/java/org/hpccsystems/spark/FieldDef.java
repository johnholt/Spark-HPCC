package org.hpccsystems.spark;

import org.hpccsystems.spark.FieldType;
import java.io.Serializable;
import java.util.Iterator;

/**
 * The name and field type for an item from the HPCC environment.  The
 * types may be single scalar types or may be arrays or structures.
 *
 * @author holtjd
 *
 */

public class FieldDef implements Serializable {
  static final long serialVersionUID = 1L;
  private String fieldName;
  private FieldType fieldType;
  private String typeName;
  private FieldDef[] defs;
  private int fields;
  //
  protected FieldDef() {
    this.fieldName = "";
    this.fieldType = FieldType.MISSING;
    this.typeName = FieldType.MISSING.description();
    this.defs = new FieldDef[0];
    this.fields = 0;
  }
  /**
   * @param fieldName the name for the field or set or structure
   * @param fieldType the type
   */
  public FieldDef(String fieldName, FieldType fieldType) {
      this.fieldName = fieldName;
      this.fieldType = fieldType;
      this.typeName = fieldType.description();
      this.defs = new FieldDef[0];
      this.fields = 1;
  }
  /**
   * @param fieldName the name of the field
   * @param fieldType the FieldType value
   * @param typeName the name of this composite type
   * @param def the array of fields composing this def
   */
  public FieldDef(String fieldName, FieldType fieldType, String typeName,
      FieldDef[] defs) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.typeName = typeName;
    this.defs = defs;
    for (int i=0; i<defs.length; i++) this.fields += defs[i].fields;
  }
  /**
   * the name of the field
   * @return the name
   */
  public String getFieldName() {
    return fieldName;
  }
  /**
   * the type of the field using the FieldType ENUM type.
   * @return the type as an enumeration value
   */
  public FieldType getFieldType() {
    return fieldType;
  }
  /**
   * A descriptive string showing the name and type.  When the
   * type is a composite, the composite definitions are included.
   * @return the string value
   */
  public String toString() {
    StringBuffer sb = new StringBuffer(this.fields*20 + 10);
    sb.append("FieldDef [fieldName=");
    sb.append(fieldName);
    sb.append(", fieldType=");
    if (this.fieldType.isComposite()) {
      sb.append("[");
      for (int i=0; i<this.defs.length; i++) {
        if (i>0) sb.append("; ");
        this.defs[i].toString();
      }
      sb.append("]");
    } else sb.append(this.typeName);
    sb.append("]");
    return sb.toString();
  }
  /**
   * The type name based upon the type enum with decorations for
   * composites.
   *@return the name of the type
   */
  public String typeName() {
    return (this.fieldType.isScalar())
        ? this.typeName
        : "RECORD(" + this.typeName + ")";
  }
  /**
   * Record name if this is a composite field
   * @return a blank name.
   */
  public String recordName() {
    return (this.fieldType.isComposite()) ? this.typeName : "";
  }
  /**
   * An iterator to walk though the type definitions that compose
   * this type.
   * @return an iterator returning FieldDef objects
   */
  public Iterator<FieldDef> getDefinitions() {
    final FieldDef[] defRef = this.defs;
    Iterator<FieldDef> rslt = new Iterator<FieldDef>() {
      int pos = 0;
      FieldDef[] copy = defRef;
      public boolean hasNext() {
        return (pos<copy.length)  ? true  : false;
      }
      public FieldDef next() {
        return copy[pos++];
      }
    };
    return rslt;
  }
}
