package org.hpccsystems.spark.data;

import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.ArrayList;
import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.spark.FieldType;
import org.hpccsystems.spark.FieldDef;
import org.hpccsystems.spark.Record;
import org.hpccsystems.spark.RecordDef;
import org.hpccsystems.spark.FilePart;
import org.hpccsystems.spark.Content;
import org.hpccsystems.spark.RecordContent;
import org.hpccsystems.spark.IntegerContent;
import org.hpccsystems.spark.RealContent;

/**
 * @author holtjd
 * Reads HPCC Cluster data in binary format.
 */
public class BinaryRecordReader implements IRecordReader {
  private RecordDef recDef;
  private int part;
  private PlainConnection pc;
  private byte[] curr;
  private int curr_pos;
  private long pos;
  private boolean active;
  //
  public BinaryRecordReader(FilePart fp, RecordDef rd) {
    this.recDef = rd;
    this.pc = new PlainConnection(fp, rd);
    this.curr = new byte[0];
    this.curr_pos = 0;
    this.active = false;
    this.pos = 0;
    this.part = fp.getThisPart();
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.data.IRecordReader#hasNext()
   */
  public boolean hasNext() throws HpccFileException {
    if (!this.active) {
      this.curr_pos = 0;
      this.active = true;
      this.curr = pc.readBlock();
    }
    if (this.curr_pos < this.curr.length) return true;
    if (pc.isClosed()) return false;
    this.curr = pc.readBlock();
    if (curr.length == 0) return false;
    this.curr_pos = 0;
    return true;
  }

  /* (non-Javadoc)
   * @see org.hpccsystems.spark.data.IRecordReader#getNext()
   */
  public Record getNext() throws HpccFileException {
    if (!this.hasNext()) {
      throw new NoSuchElementException("No next record!");
    }
    Record rslt = null;
    try {
      FieldDef fd = this.recDef.getRootDef();
      ParsedContent rec = parseRecord(this.curr, this.curr_pos, fd);
      Content[] fields = rec.getContent().asRecord();
      rslt = new Record(fields, pc.getFilename(), part, pos+curr_pos);
      this.curr_pos += rec.getConsumed();
    } catch (UnparsableContentException e) {
      throw new HpccFileException("Failed to parse next record", e);
    }
    return rslt;
  }
  /**
   * Parse the byte array starting at position start for a record
   * data object of the layout specified by def.
   * @param src the source byte array of the data from the HPCC cluster
   * @param start the start position in the buffer
   * @param def the field definition for the Record definition
   * @return a ParsedContent container
   * @throws UnparsableContentException
   */
  private static ParsedContent parseRecord(byte[] src, int start, FieldDef def)
        throws UnparsableContentException {
    Iterator<FieldDef> iter = def.getDefinitions();
    ArrayList<Content> fields = new ArrayList<Content>(def.getNumFields());
    byte[] bwork = new byte[8];
    int consumed = 0;
    Content x = null;
    while (iter.hasNext()) {
      FieldDef fd = iter.next();
      if (start+consumed+fd.getDataLen() > src.length) {
        StringBuilder sb = new StringBuilder();
        sb.append("Data ended prematurely parsing field ");
        sb.append(fd.getFieldName());
        throw new UnparsableContentException(sb.toString());
      }
      for (int i=0; i<8; i++) bwork[i] = 0;
      switch (fd.getFieldType()) {
        case INTEGER:
          long v = getInt(src, start+consumed, fd.getDataLen(),
                          fd.getSourceType() == HpccSrcType.LITTLE_ENDIAN);
          x = new IntegerContent(fd.getFieldName(), v);
          fields.add(x);
          consumed += fd.getDataLen();
          break;
        case REAL:
          for (int i=0; i<fd.getDataLen(); i++) {
            if (fd.getSourceType() == HpccSrcType.BIG_ENDIAN) {
              bwork[i] = src[i+start+consumed];
            } else {
              bwork[i] = src[start+consumed+fd.getDataLen()-1-i];
            }
          }
          double u = getReal(src, start+consumed, fd.getDataLen(),
                            fd.getSourceType() == HpccSrcType.LITTLE_ENDIAN);
          x = new RealContent(fd.getFieldName(), u);
          fields.add(x);
          consumed += fd.getDataLen();
          break;
        default:
          break;
      }
    }
    RecordContent rc = new RecordContent(def.getFieldName(),
                                         fields.toArray(new Content[0]));
    ParsedContent rslt = new ParsedContent(rc, consumed);
    return rslt;
  }
  private static long getInt(byte[] b, int pos, int len, boolean little_endian) {
    long v = 0;
    for (int i=0; i<len; i++) {
      v = (v << 8) |
          (((long)(b[pos + ((little_endian) ? len-1-i  : i)] & 0xff)));
    }
    return v;
  }
  private static double getReal(byte[] b, int pos, int len, boolean little_endian) {
    double u = 0;
    if (len == 4) {
      int u4 = 0;
      for (int i=0; i<4; i++) {
        u4 = (u4 << 8) |
            (((int)(b[pos + ((little_endian) ? len-1-i  : i)] & 0xff)));
      }
      u = Float.intBitsToFloat(u4);
    } else if (len == 8) {
      long u8 = 0;
      for (int i=0; i<8; i++) {
        u8 = (u8 << 8) |
            (((long)(b[pos + ((little_endian) ? len-1-i  : i)] & 0xff)));
      }
      u = Double.longBitsToDouble(u8);
    }
    return u;
  }
}
