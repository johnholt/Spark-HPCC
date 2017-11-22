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
  private String fileName;
  private int part;
  private long pos;
  private java.util.HashMap<String, FieldContent> content;
  /**
   * Record from an array of content items
   * @param content
   * @param fileName name of the file
   * @param part the file part number
   * @param pos the position of this record in the file part
   */
  public Record(FieldContent[] content, String fileName, int part, long pos) {
    this.fileName = fileName;
    this.part = part;
    this.pos = pos;
    this.content = new java.util.HashMap<String, FieldContent>(100);
    for (FieldContent w : content) {
      this.content.put(w.getName(), w);
    }
  }
  /**
   * No argument constructor for serialization support
   */
  protected Record() {
    this.fileName = "";
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
  /**
   * Copy the record fields into an array
   * @return an array of content items
   */
  public FieldContent[] getFields() {
    java.util.Collection<FieldContent> w = content.values();
    FieldContent[] rslt = w.toArray(new FieldContent[0]);
    return rslt;
  }
  /**
   * The name of this file
   * @return the name
   */
  public String fileName() { return this.fileName; }
  /**
   * @return the part number of this part of the file
   */
  public int getFilePart() { return this.part; }
  /**
   * The relative record position of this record within the file part
   * @return record position
   */
  public long getPos() { return pos; }
}
