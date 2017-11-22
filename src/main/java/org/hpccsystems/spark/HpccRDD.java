package org.hpccsystems.spark;

import java.io.Serializable;
import java.util.Iterator;
import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.InterruptibleIterator;
import scala.collection.Seq;
import scala.collection.mutable.ArraySeq;
import scala.reflect.ClassTag;
import scala.reflect.ClassTag$;
import scala.collection.JavaConverters;

/**
 * @author holtjd
 *
 */
public class HpccRDD extends RDD<Record> implements Serializable {
  private static final long serialVersionUID = 1L;
  private static final ClassTag<Record> CT_RECORD
                          = ClassTag$.MODULE$.apply(Record.class);
  private static Seq empty
          = new ArraySeq<Dependency<RDD<Record>>>(0);
  //
  private FilePart[] parts;
  private RecordDef def;
  /**
   * @param _sc
   * @param
   */
  public HpccRDD(SparkContext _sc, FilePart[] parts, RecordDef def) {
    super(_sc, empty, CT_RECORD);
    this.parts = new FilePart[parts.length];
    for (int i=0; i<parts.length; i++) {
      this.parts[i] = parts[i];
    }
    this.def = def;
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#compute(org.apache.spark.Partition, org.apache.spark.TaskContext)
   */
  @Override
  public InterruptibleIterator<Record> compute(Partition p_arg, TaskContext ctx) {
    final FilePart this_part = (FilePart) p_arg;
    final RecordDef def = this.def;
    Iterator<Record> iter = new Iterator<Record>() {
      private boolean first_pending = true;
      private boolean eof = false;
      private HpccRemoteFileReader rfr
                      = new HpccRemoteFileReader(def, this_part);
      private Record latest;
      //
      private Record readRemote() {
        Record rslt = null;
        try {
          rslt = rfr.remoteRead();
        } catch (java.io.EOFException e) {
          eof = true;
        }
        return rslt;
      }
      public boolean hasNext() {
        if (first_pending) {
          latest = readRemote();
        }
        first_pending = false;
        return eof;
      }
      public Record next() {
        if (first_pending) {
          latest = readRemote();
          first_pending = false;
        }
        Record rslt = latest;
        latest = readRemote();
        return rslt;
      }
    };
    scala.collection.Iterator<Record> s_iter
        = JavaConverters.asScalaIteratorConverter(iter).asScala();
    InterruptibleIterator<Record> rslt
        = new InterruptibleIterator<Record>(ctx, s_iter);
    return rslt;
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#getPartitions()
   */
  @Override
  public Partition[] getPartitions() {
    FilePart[] rslt = new FilePart[this.parts.length];
    for (int i=0; i<this.parts.length; i++) rslt[i] = this.parts[i];
    return rslt;
  }

}
