/**
 *
 */
package org.hpccsystems.spark;


/**
 * A data record from the HPCC system.  A collection of fields, accessed by
 * name or as an enumeration.
 *
 * @author holtjd
 *
 */
public class Record implements java.io.Serializable {
  static final long serialVersionUID = 1L;
  private String recordName;
  private java.util.HashMap<String, FieldContent> content;
  /**
   * Record from an array of content items
   * @param content
   * @param typeName name of the record type
   */
  public Record(FieldContent[] content, String typeName) {
    this.recordName = typeName;
    this.content = new java.util.HashMap<String, FieldContent>(100);
    for (FieldContent w : content) {
      this.content.put(w.getName(), w);
    }
  }
  /**
   * A record with no name, a top level record
   * @param content
   */
  public Record(FieldContent[] content) {
    this(content, "");
  }
  /**
   * No argument constructor for serialization support
   */
  protected Record() {
    this.recordName = "";
    this.content = new java.util.HashMap<String, FieldContent>(0);
  }
  /**
   * Get a field by the name.
   * @param name
   * @return
   */
  public FieldContent getFieldContent(String name) {
    return content.get(name);
  }
  public FieldContent[] getFields() {
    java.util.Collection<FieldContent> w = content.values();
    FieldContent[] rslt = w.toArray(new FieldContent[0]);
    return rslt;
  }
}
