package org.hpccsystems.spark;

import java.io.Serializable;

import org.hpccsystems.spark.thor.FieldDef;

/**
 * @author holtjd
 * The row content, an group of Content objects.
 */
public class RecordContent extends Content implements Serializable{
  final private static long serialVersionUID = 1L;
  private Content[] items;
  /**
   * Empty constructor for serialization
   */
  protected RecordContent() {
    this.items = new Content[0];
  }
  /**
   * Make a deep copy of the source object
   * @param src the content to be copied
   */
  protected RecordContent(RecordContent src) {
    this.items = new Content[src.items.length];
    for (int i=0; i<src.items.length; i++) this.items[i] = src.items[i];
  }
  /**
   * Constructor used when a FieldDef is not available,
   * @param name
   * @param fields the content fields
   */
  public RecordContent(String name, Content[] fields) {
    super(FieldType.RECORD, name);
    this.items = new Content[fields.length];
    for (int i=0; i<fields.length; i++) this.items[i] = fields[i];
  }
  /**
   * Normally used constructor.
   * @param def the fieldDef for the record
   * @param fields the record content fields
   */
  public RecordContent(FieldDef def, Content[] fields) {
    super(def);
    if (def.getFieldType()!=FieldType.RECORD) {
      throw new IllegalArgumentException("Type of def must be record");
    }
    this.items = new Content[fields.length];
    for (int i=0; i<fields.length; i++) this.items[i] = fields[i];
  }
  /**
   * As a copy of the fields.
   * @return
   */
  public Content[] asFieldArray() {
    Content[] rslt = new Content[this.items.length];
    for (int i=0; i<this.items.length; i++) rslt[i] = this.items[i];
    return rslt;
  }
  /**
   * Get the content item at position ndx in the array
   * @param ndx
   * @return
   */
  public Content fieldAt(int ndx) {
    return this.items[ndx];
  }
  public java.util.Iterator<Content> asIterator() {
    Content[] t = this.items;
    return new java.util.Iterator<Content>() {
      private final Content[] cp = t;
      private int pos = 0;
      public boolean hasNext() { return pos<cp.length; }
      public Content next() throws java.util.NoSuchElementException {
        if (pos >= cp.length) throw new java.util.NoSuchElementException();
        return cp[pos++];
      }
    };
  }
  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    return items.length;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString()
   */
  @Override
  public String asString(String fieldSep, String elementSep) {
    StringBuilder sb = new StringBuilder();
    for (int i=0; i<items.length; i++) {
      if (i>0) sb.append(fieldSep);
      sb.append(items[i].toString());
    }
    return sb.toString();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfString()
   */
  @Override
  public String[] asSetOfString() {
    String[] rslt = new String[1];
    rslt[0] = this.asString();
    return rslt;
  }

}
