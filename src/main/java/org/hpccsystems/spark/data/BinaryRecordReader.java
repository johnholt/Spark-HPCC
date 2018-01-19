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
          for (int i=0; i < fd.getDataLen(); i++) {
            if (fd.getSourceType() == HpccSrcType.LITTLE_ENDIAN) {
              bwork[7-i] = src[i+consumed+start];
            } else {
              bwork[i+8-fd.getDataLen()] = src[i+consumed+start];
            }
          }
          long v = (((long)(bwork[0] & 0xff) << 56) |
                    ((long)(bwork[1] & 0xff) << 48) |
                    ((long)(bwork[2] & 0xff) << 40) |
                    ((long)(bwork[3] & 0xff) << 32) |
                    ((long)(bwork[4] & 0xff) << 24) |
                    ((long)(bwork[5] & 0xff) << 16) |
                    ((long)(bwork[6] & 0xff) <<  8) |
                    ((long)(bwork[7]))  );
          x = new IntegerContent(fd.getFieldName(), v);
          fields.add(x);
          consumed += fd.getDataLen();
          break;
        case REAL:
          for (int i=0; i<fd.getDataLen(); i++) {
            if (fd.getSourceType() == HpccSrcType.BIG_ENDIAN) {
              bwork[i] = src[i+consumed];
            } else {
              bwork[i] = src[start+consumed+fd.getDataLen()-1-i];
            }
          }
          double u = 0;
          if (fd.getDataLen()==4) {
            int u4 = (((int)(bwork[0] & 0xff) << 24) |
                      ((int)(bwork[1] & 0xff) << 16) |
                      ((int)(bwork[2] & 0xff) <<  8) |
                      ((int)(bwork[3])));
            u = Float.intBitsToFloat(u4);
          } else {
            long u8 = (((long)(bwork[0] & 0xff) << 56) |
                       ((long)(bwork[1] & 0xff) << 48) |
                       ((long)(bwork[2] & 0xff) << 40) |
                       ((long)(bwork[3] & 0xff) << 32) |
                       ((long)(bwork[4] & 0xff) << 24) |
                       ((long)(bwork[5] & 0xff) << 16) |
                       ((long)(bwork[6] & 0xff) <<  8) |
                       ((long)(bwork[7]))  );
            u = Double.longBitsToDouble(u8);
          }
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
}
