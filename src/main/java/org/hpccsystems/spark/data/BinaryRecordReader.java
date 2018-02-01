package org.hpccsystems.spark.data;

import java.util.NoSuchElementException;
import java.util.Iterator;
import java.util.ArrayList;
import java.util.Arrays;
import java.nio.charset.Charset;
import org.hpccsystems.spark.HpccFileException;
import org.hpccsystems.spark.FieldDef;
import org.hpccsystems.spark.Record;
import org.hpccsystems.spark.RecordDef;
import org.hpccsystems.spark.FilePart;
import org.hpccsystems.spark.Content;
import org.hpccsystems.spark.RecordContent;
import org.hpccsystems.spark.IntegerContent;
import org.hpccsystems.spark.RealContent;
import org.hpccsystems.spark.BinaryContent;
import org.hpccsystems.spark.BooleanContent;
import org.hpccsystems.spark.StringContent;

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
  private static final Charset sbcSet = Charset.forName("ISO-8859-1");
  private static final Charset utf8Set = Charset.forName("UTF-8");
  private static final Charset utf16beSet = Charset.forName("UTF-16BE");
  private static final Charset utf16leSet = Charset.forName("UTF-16LE");
  //
  /**
   * A Binary record reader.
   * @param fp the file part to be read
   * @param rd the record def
   */
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
      Content w = rec.getContent();
      if (!(w instanceof RecordContent)) {
        throw new HpccFileException("RecordContent not found");
      }
      Content[] fields = ((RecordContent)w).asFieldArray();
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
    int consumed = 0;
    int dataLen = 0;
    int dataStart = 0;
    while (iter.hasNext()) {
      FieldDef fd = iter.next();
      int testLength = (fd.isFixed())  ? fd.getDataLen()   : 4;
      if (start+consumed+testLength > src.length) {
        StringBuilder sb = new StringBuilder();
        sb.append("Data ended prematurely parsing field ");
        sb.append(fd.getFieldName());
        throw new UnparsableContentException(sb.toString());
      }
      // Embedded field lengths are little endian
      switch (fd.getFieldType()) {
        case INTEGER:
          long v = getInt(src, start+consumed, fd.getDataLen(),
                          fd.getSourceType() == HpccSrcType.LITTLE_ENDIAN);
          fields.add(new IntegerContent(fd.getFieldName(), v));
          consumed += fd.getDataLen();
          break;
        case REAL:
          double u = getReal(src, start+consumed, fd.getDataLen(),
                            fd.getSourceType() == HpccSrcType.LITTLE_ENDIAN);
          fields.add(new RealContent(fd.getFieldName(), u));

          consumed += fd.getDataLen();
          break;
        case BINARY:
          if (fd.isFixed()) dataLen = fd.getDataLen();
          else {
            dataLen = (int)getInt(src, start+consumed, 4, true);
            consumed += 4;
          }
          dataStart = start+consumed;
          if (dataLen+dataStart > src.length) {
            throw new UnparsableContentException("Data ended prematurely");
          }
          byte[] bytes = Arrays.copyOfRange(src, dataStart, dataStart+dataLen);
          fields.add(new BinaryContent(fd.getFieldName(), bytes));
          consumed += dataLen;
          break;
        case BOOLEAN:
          boolean flag = false;
          for (int i=0; i<fd.getDataLen(); i++) {
            flag = flag | (src[start+consumed+i] == 0) ? false  : true;
          }
          fields.add(new BooleanContent(fd.getFieldName(), flag));
          consumed += fd.getDataLen();
          break;
        case STRING:
          if (fd.isFixed()) {
            dataLen = getCodeUnits(fd.getSourceType(), src, start+consumed,
                                   fd.getDataLen());
          } else {
            int cp = ((int)getInt(src, start+consumed, 4, true));
            dataLen = getCodeUnits(fd.getSourceType(), src, start+consumed+4, cp);
            consumed += 4;
          }
          if (start+consumed+dataLen > src.length) {
            throw new UnparsableContentException("String data ended early");
          }
          String s = getString(fd.getSourceType(), src, start+consumed, dataLen);
          fields.add(new StringContent(fd.getFieldName(), s));
          consumed += dataLen;
          break;
        case RECORD:
          throw new UnparsableContentException("No record parse available");
        case SET_OF_INTEGER:
          throw new UnparsableContentException("No integer set available");
        case SET_OF_REAL:
          throw new UnparsableContentException("No real set available");
        case SET_OF_BINARY:
          throw new UnparsableContentException("No binary set available");
        case SET_OF_BOOLEAN:
          throw new UnparsableContentException("No set of boolean available");
        case SET_OF_STRING:
          throw new UnparsableContentException("No set of string available");
        case SET_OF_RECORD:
          throw new UnparsableContentException("No set of record available");
        default:
          String msg = "Unhandled type: " + fd.getFieldType().toString();
          throw new UnparsableContentException(msg);
      }
    }
    RecordContent rc = new RecordContent(def.getFieldName(),
                                         fields.toArray(new Content[0]));
    ParsedContent rslt = new ParsedContent(rc, consumed);
    return rslt;
  }
  /**
   * Get an integer from the byte array
   * @param b the byte array from the HPCC THOR node
   * @param pos the position in the array
   * @param len the length, 1 to 8 bytes
   * @param little_endian true if the value is little endian
   * @return the integer extracted as a long
   */
  private static long getInt(byte[] b, int pos, int len, boolean little_endian) {
    long v = 0;
    for (int i=0; i<len; i++) {
      v = (v << 8) |
          (((long)(b[pos + ((little_endian) ? len-1-i  : i)] & 0xff)));
    }
    return v;
  }
  /**
   * Get a real from the byte array
   * @param b the byte array of data from the THOR node
   * @param pos the position in the array
   * @param len the length, 4 or 8
   * @param little_endian true if the value is little endian
   * @return the extracted real as a double
   */
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
  /**
   * Extract a string from the byte array
   * @param styp the source type in the byte array
   * @param b the byte array from the THOR node
   * @param pos the position in the array
   * @param len the number of bytes
   * @return the extracted string
   */
  private static String getString(HpccSrcType styp, byte[] b, int pos, int len)
          throws UnparsableContentException {
    String rslt = "";
    switch (styp) {
      case UTF8:
        rslt = new String(b, pos, len, utf8Set);
        break;
      case SINGLE_BYTE_CHAR:
        rslt = new String(b, pos, len, sbcSet);
        break;
      case UTF16BE:
        rslt = new String(b, pos, len, utf16beSet);
        break;
      case UTF16LE:
        rslt = new String(b, pos, len, utf16leSet);
        break;
      default:
        throw new UnparsableContentException("Unknown source type");
    }
    return rslt;
  }
  /**
   * Get the number of code units (number of bytes) used to encode cp coded
   * characters.
   * @param styp the source data type
   * @param b the byte array buffer
   * @param pos the current position in the buffer
   * @param cp the number of code points.
   * @return the number of bytes
   * @throws UnparsableContentException when the end of the buffer was reach
   * unexpected or the stream of data was incorrect, such as an illegal byte
   * sequence for UTF8.
   */
  private static int getCodeUnits(HpccSrcType styp, byte[] b, int pos, int cp)
        throws UnparsableContentException {
    int bytes = 0;
    int work = 0;
    switch (styp) {
      case UTF8:
        for (int i=0; i<cp && pos+bytes<b.length; i++) {
          if ((b[pos+bytes] & 0x80) == 0) bytes++;
          else if ((b[pos+bytes] & 0xE0) == 0xC0) bytes+=2;
          else if ((b[pos+bytes] & 0xF0) == 0xE0) bytes+=3;
          else if ((b[pos+bytes] & 0xF8) == 0xF0) bytes+=4;
          else throw new UnparsableContentException("Illegal UTF-8 sequence");
        }
        break;
      case SINGLE_BYTE_CHAR:
        bytes = cp;
        break;
      case UTF16BE:
        if (pos+(cp*2) > b.length) {
          throw new UnparsableContentException("Early end of data");
        }
        work = (int) getInt(b, pos+((cp-1)*2), 2, false);
        // check the last character to make sure it is not a truncated pair
        if (Character.isHighSurrogate((char)work)) bytes=(cp-1)*2;  // truncated?
        else bytes = cp*2;  // did not end on a truncated pair
        break;
      case UTF16LE:
        if (pos+(cp*2) > b.length) {
          throw new UnparsableContentException("Early end of data");
        }
        work = (int) getInt(b, pos+((cp-1)*2), 2, true);
        // check the last character to make sure it is not a truncated pair
        if (Character.isHighSurrogate((char)work)) bytes=(cp-1)*2;  // truncated?
        else bytes = cp * 2; // did not end on a truncated pair.
        break;
      default:
        StringBuilder sb = new StringBuilder();
        sb.append("Unknown data source type for a string of: ");
        sb.append(styp.toString());
        throw new UnparsableContentException(sb.toString());
    }
    return bytes;
  }
}
