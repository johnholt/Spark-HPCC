/**
 *
 */
package org.hpccsystems.spark;

import java.util.Iterator;
/**
 * Composite types are Record (Structure) types or sets of record types.
 * @author holtjd
 *
 */
public class CompositeDef extends FieldDef {
  private static final long serialVersionUID = 1L;
  private String typeName;
  private FieldDef[] defs;
  protected CompositeDef() {
    super();
    this.typeName = "";
    this.defs = new FieldDef[0];
  }
  /**
   * @param fieldName the name of the field
   * @param fieldType the FieldType value
   * @param typeName the name of this composite type
   * @param def the array of fields composing this def
   */
  public CompositeDef(String fieldName, FieldType fieldType, String typeName,
      FieldDef[] defs) {
    super(fieldName, fieldType);
    this.typeName = typeName;
    this.defs = defs;
  }
  public String typeName() { return "RECORD("+this.typeName+")"; }
  public Iterator<FieldDef> defIter() {
    FieldDef[] defRef = this.defs;
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
