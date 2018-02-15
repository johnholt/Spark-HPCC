package org.hpccsystems.spark;

import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.linalg.Vector;

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
  private java.util.HashMap<String, Content> content;
  /**
   * Record from an array of content items
   * @param content
   * @param fileName name of the file
   * @param part the file part number
   * @param pos the position of this record in the file part
   */
  public Record(Content[] fields, String fileName, int part, long pos) {
    this.fileName = fileName;
    this.part = part;
    this.pos = pos;
    this.content = new java.util.HashMap<String, Content>(100);
    for (Content w : fields) {
      this.content.put(w.getName(), w);
    }
  }
  /**
   * No argument constructor for serialization support
   */
  protected Record() {
    this.fileName = "";
    this.content = new java.util.HashMap<String, Content>(0);
  }
  /**
   * Construct a labeled point record from an HPCC record.
   * @param labelName name of the content field to use as a label
   * @param dimNames names of the fields to use as the vector dimensions,
   * in the order specified.
   * @return a lebeled point record
   * @throws java.lang.IllegalArgumentException when a field name does not exist
   */
  public LabeledPoint asLabeledPoint(String labelName, String[] dimNames)
        throws IllegalArgumentException {
    if (!content.containsKey(labelName)) {
      throw new IllegalArgumentException(labelName + " not a record field.");
    }
    Content cv = content.get(labelName);
    double label = cv.getRealValue();
    Vector features = this.asMlLibVector(dimNames);
    LabeledPoint rslt = new LabeledPoint(label, features);
    return rslt;
  }
  /**
   * Construct an MLLib dense vector from the HPCC record.
   * @param dimNames the names of the fields to use as the dimensions
   * @return a dense vector
   * @throws java.lang.IllegalArgumentException when the field name does not exist
   */
  public Vector asMlLibVector(String[] dimNames)
        throws java.util.NoSuchElementException {
    double[] features = new double[dimNames.length];
    for (int i=0; i<dimNames.length; i++) {
      if (!content.containsKey(dimNames[i])) {
        throw new IllegalArgumentException(dimNames[i] + " not a record field");
      }
      Content cv = content.get(dimNames[i]);
      features[i] = cv.getRealValue();
    }
    Vector rslt = Vectors.dense(features);
    return rslt;
  }
  /**
   * Get a field by the name.
   * @param name
   * @return
   */
  public Content getFieldContent(String name) {
    return content.get(name);
  }
  /**
   * Copy the record fields into an array
   * @return an array of content items
   */
  public Content[] getFields() {
    java.util.Collection<Content> w = content.values();
    Content[] rslt = w.toArray(new Content[0]);
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
  /**
   * A form of the record for display.
   * @return display string
   */
  public String toString() {
    StringBuilder sb = new StringBuilder();
    sb.append("Record at position ");
    sb.append(this.getFilePart());
    sb.append(":");
    sb.append( Long.toString(this.getPos()));
    sb.append(" = ");
    for (Content fld : this.getFields()) sb.append(fld.toString());
    return sb.toString();
  }
}
