package org.hpccsystems.spark;

/**
 * @author holtjd
 * The row content, an group of Content objects.
 */
public class RecordContent extends Content {
  final private static long serialVersionUID = 1L;
  private Content[] items;
  /**
   * Empty constructor for serialization
   */
  private RecordContent() {
    this.items = new Content[0];
  }
  /**
   * Make a deep copy of the source object
   * @param src the content to be copied
   */
  private RecordContent(RecordContent src) {
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

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#numFields()
   */
  @Override
  public int numFields() {
    return items.length;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asInt()
   */
  @Override
  public long asInt() {
    return 0;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfInt()
   */
  @Override
  public long[] asSetOfInt() {
    long[] rslt = new long[0];
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asReal()
   */
  @Override
  public double asReal() {
    return 0;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfReal()
   */
  @Override
  public double[] asSetOfReal() {
    double[] rslt = new double[0];
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asString()
   */
  @Override
  public String asString() {
    StringBuilder sb = new StringBuilder();
    for (Content fld : this.items) {
      sb.append("{");
      sb.append(fld.toString());
      sb.append("}");
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

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asRecord()
   */
  @Override
  public Content[] asRecord() {
    Content[] rslt = new Content[this.items.length];
    for (int i=0; i<this.items.length; i++) rslt[i] = this.items[i];
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.Content#asSetOfRecord()
   */
  @Override
  public RecordContent[] asSetOfRecord() {
    RecordContent[] rslt = new RecordContent[1];
    rslt[0] = new RecordContent(this);
    return rslt;
  }
  @Override
  public byte[] asBinary() {
    byte[] rslt = new byte[0];
    return rslt;
  }
  @Override
  public byte[][] asSetOfBinary() {
    byte[][] rslt = new byte[1][];
    rslt[0] = this.asBinary();
    return rslt;
  }

}
