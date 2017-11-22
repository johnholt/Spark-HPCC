package org.hpccsystems.spark;

import java.util.ArrayList;
/**
 * @author holtjd
 * Remote file reader used by the HpccRDD
 */
public class HpccRemoteFileReader {
  private RecordDef def;
  private FilePart fp;
  private boolean eof;
  private long pos;
  /**
   * A remote file reader that reads the part identified by the
   * FilePart object using the record definition provided.
   * @param def the defintion of the data
   * @param fp the part of the file, name and location
   */
  public HpccRemoteFileReader(RecordDef def, FilePart fp) {
    this.def = def;
    this.fp = fp;
    this.eof = fp.getPartSize() == 0;
    this.pos = 0;
  }
  public boolean eof() { return eof; }
  public Record remoteRead() throws java.io.EOFException {
    if (eof || pos>fp.getPartSize()) throw new java.io.EOFException("HPCC file partition at EOF");
    pos += 100;
    ArrayList<FieldContent> test_data = new ArrayList<FieldContent>();
    String field1 = "File part "+ String.valueOf(fp.getThisPart());
    test_data.add(new StringContent("F1", field1));
    String field2 = "pos " + String.valueOf(pos);
    test_data.add(new StringContent("F2", field2));
    FieldContent[] content = test_data.toArray(new FieldContent[0]);
    Record rslt = new Record(content, fp.getFilename(), fp.getThisPart(), pos);
    return rslt;
  }
}
