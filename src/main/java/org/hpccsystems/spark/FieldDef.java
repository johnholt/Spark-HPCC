package org.hpccsystems.spark;

import org.hpccsystems.spark.FieldType;
import org.hpccsystems.spark.data.DefToken;
import org.hpccsystems.spark.data.UnusableDataDefinitionException;
import org.hpccsystems.spark.data.HpccSrcType;

import com.fasterxml.jackson.core.JsonToken;

import java.io.Serializable;
import java.util.Iterator;
import java.util.HashMap;

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
  private HpccSrcType srcType;
  private int fields;
  private int len;
  private int childLen;
  private boolean fixedLength;
  //
  private static final String FieldNameName = "name";
  private static final String FieldTypeName = "type";
  //
  protected FieldDef() {
    this.fieldName = "";
    this.fieldType = FieldType.MISSING;
    this.typeName = FieldType.MISSING.description();
    this.defs = new FieldDef[0];
    this.srcType = HpccSrcType.UNKNOWN;
    this.fields = 0;
    this.len = 0;
    this.childLen = 0;
    this.fixedLength = false;
  }
  /**
   * @param fieldName the name for the field or set or structure
   * @param fieldDef the type definition
   */
  public FieldDef(String fieldName, TypeDef typeDef) {
      this.fieldName = fieldName;
      this.fieldType = typeDef.getType();
      this.typeName = typeDef.description();
      this.defs = typeDef.getStructDef();
      this.srcType = typeDef.getSourceType();
      this.fields = 1;
      this.len = typeDef.getLength();
      this.childLen = typeDef.childLen();
      this.fixedLength = typeDef.isFixedLength();
  }
  /**
   * @param fieldName the name of the field
   * @param fieldType the FieldType value
   * @param typeName the name of this composite type
   * @param len the field length
   * @param childLen the child field length or zero
   * @param isFixedLength len may be non-zero and variable
   * @param def the array of fields composing this def
   */
  public FieldDef(String fieldName, FieldType fieldType, String typeName, long len,
      long childLen, boolean isFixedLength, HpccSrcType styp, FieldDef[] defs) {
    this.fieldName = fieldName;
    this.fieldType = fieldType;
    this.typeName = typeName;
    this.defs = defs;
    this.srcType = styp;
    this.fields = defs.length;
    this.fixedLength = isFixedLength;
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
   * Data type on the HPCC cluster.
   * @return type enumeration
   */
  public HpccSrcType getSourceType() { return this.srcType; }
  /**
   * Length of the data or minimum length if variable
   * @return length
   */
  public int getDataLen() { return this.len; }
  /**
   * Length of the child definition or minimum length if variable
   * @return length
   */
  public int getChildLen() { return this.childLen; }
  /**
   * Fixed of variable length
   * @return true when fixed length
   */
  public boolean isFixed() { return this.fixedLength; }

  /**
   * A descriptive string showing the name and type.  When the
   * type is a composite, the composite definitions are included.
   * @return the string value
   */
  public String toString() {
    StringBuffer sb = new StringBuffer(this.fields*20 + 10);
    sb.append("FieldDef [fieldName=");
    sb.append(this.fieldName);
    sb.append(", ");
    sb.append((this.fixedLength) ? "F len="  : "V len=");
    sb.append(len);
    if (childLen > 0) {
      sb.append(":");
      sb.append(this.childLen);
    }
    sb.append(", fieldType=");
    if (this.fieldType.isComposite()) {
      sb.append("{");
      sb.append(this.fields);
      sb.append("}[");
      for (int i=0; i<this.defs.length; i++) {
        if (i>0) sb.append("; ");
        sb.append(this.defs[i].toString());
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
    return (this.fieldType.isScalar() || this.fieldType.isVector())
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
   * The number of fields, 1 or more if a record
   * @return number of fields.
   */
  public int getNumFields() { return this.fields; }
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
  /**
   * Pick up a field definition from the JSON record definiton string.
   * The definitions are objects in the fields JSON array pair.  The
   * objects have name, type name, flags, and xpath pairs.  The flags
   * and xpath pairs are ignored.
   *
   * Start with a START_OBJECT and return on an END_OBJECT.  An exception
   * is thrown if not true or if name or type pairs are missing.
   *
   * @param first the first token in the sequence, must be START_OBJECT.
   * @param toks_iter an itreator of the tokens from a JSON record def string
   * @param type_dict the dictionary of types defined earlier in the string
   * @return the field defintion
   */
  public static FieldDef parseDef(DefToken first,
            Iterator<DefToken> toks_iter,
            HashMap<String, TypeDef> type_dict)
      throws UnusableDataDefinitionException {
    if (first.getToken() != JsonToken.START_OBJECT) {
      StringBuilder sb = new StringBuilder();
      sb.append("Expected start of object, found ");
      sb.append(first.getToken().toString());
      throw new UnusableDataDefinitionException(sb.toString());
    }
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination");
    }
    DefToken curr = toks_iter.next();
    String fieldName = "";
    String typeName = "";
    while(toks_iter.hasNext() && curr.getToken() != JsonToken.END_OBJECT) {
      if (FieldNameName.equals(curr.getName())) {
        fieldName = curr.getString();
      }
      if (FieldTypeName.equals(curr.getName())) {
        typeName = curr.getString();
      }
      curr = toks_iter.next();
    }
    if (!toks_iter.hasNext()) {
      throw new UnusableDataDefinitionException("Early termination");
    }
    if (fieldName.equals("") || typeName.equals("")) {
      throw new UnusableDataDefinitionException("Missing name or type pairs");
    }
    if (!type_dict.containsKey(typeName)) {
      StringBuilder sb = new StringBuilder();
      sb.append("Type name ");
      sb.append(typeName);
      sb.append(" used but not defined.");
      throw new UnusableDataDefinitionException(sb.toString());
    }
    TypeDef typ = type_dict.get(typeName);
    FieldDef rslt = new FieldDef(fieldName, typ);
    return rslt;
  }
}
