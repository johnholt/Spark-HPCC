/**
 *
 */
package org.hpccsystems.spark;

/**
 * A field content item with String as the native type.
 * @author holtjd
 *
 */
public class StringContent extends FieldContent {
  private static final long serialVersionUID = 1L;
  private String value;
  /**
   * No argument constructor for serialization
   */
  protected StringContent() {
    super();
    this.value = "";
  }
  /**
   * Construct a StringContent item
   * @param v the string value
   * @param name the name of the field
   */
  public StringContent(String name, String v) {
    super(FieldType.STRING, name);
    this.value = v;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asData()
   */
  @Override
  public byte[] asData() {
    // TODO Auto-generated method stub
    return null;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asInt()
   */
  @Override
  public long asInt() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asReal()
   */
  @Override
  public double asReal() {
    // TODO Auto-generated method stub
    return 0;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.FieldContent#asString()
   */
  @Override
  public String asString() {
    return this.value;
  }

}
