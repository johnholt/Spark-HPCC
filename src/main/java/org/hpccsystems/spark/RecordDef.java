package org.hpccsystems.spark;

import java.io.Serializable;

/**
 * HPCC record definiton.  Includes HPCC record info strings and derived
 * Field Defs.
 * @author holtjd
 *
 */
public class RecordDef implements Serializable {
  private static final long serialVersionUID = 1l;
  private String input_def;
  private FieldDef root;
  private String output_def;
  /**
   * Empty constructor for serialization
   */
  protected RecordDef() {
    this.root = new FieldDef("root", FieldType.MISSING);
    this.output_def = "";
    this.input_def = "";
  }
  /**
   * Construct a record definition.  Normally used by the static
   * function parseJsonDef.
   * @param def the Json string used as the input and default output
   * definition.
   * @param root the definition parsed into FieldDef objects.  The input
   * is the root definition for the record.
   */
  protected RecordDef(String def, FieldDef root) {
    this.input_def = def;
    this.output_def = def;  // default output is all content
    this.root = root;
  }
  /**
   * Create a record definitojn object from the JSON definition
   * string.
   * @param def the JSON record type defintion returned from WsDfu
   * @return a new record definition
   */
  static public RecordDef parseJsonDef(String def) {
    FieldDef root = new FieldDef("root", FieldType.MISSING);
    RecordDef rslt = new RecordDef(def, root);
    return rslt;
  }
  /**
   * The definition of the data for the remote file reader
   * @return the definition
   */
  public String getJsonInputDef() { return input_def; }
  /**
   * Get the JSON string that defines the output structure for the remote
   * reader.
   * @return the output definition
   */
  public String getJsonOutputDef() { return output_def; }
  /**
   * Replace the current output definition.
   * @param new_def the new definition, a JSON string
   * @return the prior definition
   */
  public String setJsonOutputDef(String new_def) {
    String old = this.output_def;
    this.output_def = new_def;
    return old;
  }
  /**
   * The record definition object
   * @return root definition
   */
  public FieldDef getRootDef() { return root; }
}
