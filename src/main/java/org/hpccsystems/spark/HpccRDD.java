/**
 *
 */
package org.hpccsystems.spark;

import java.io.Serializable;

import org.apache.spark.Dependency;
import org.apache.spark.Partition;
import org.apache.spark.SparkContext;
import org.apache.spark.TaskContext;
import org.apache.spark.rdd.RDD;
import org.apache.spark.InterruptibleIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.NoSuchElementException;

//import scala.collection.Iterator;
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
  /**
   * @param _sc
   * @param
   */
  public HpccRDD(SparkContext _sc, FilePart[] parts) {
    super(_sc, empty, CT_RECORD);
    this.parts = new FilePart[parts.length];
    for (int i=0; i<parts.length; i++) {
      this.parts[i] = parts[i];
    }
  }

  /* (non-Javadoc)
   * @see org.apache.spark.rdd.RDD#compute(org.apache.spark.Partition, org.apache.spark.TaskContext)
   */
  @Override
  public InterruptibleIterator<Record> compute(Partition p_arg, TaskContext ctx) {
    FilePart this_part = (FilePart) p_arg;
    Iterator<Record> iter = new Iterator<Record>() {
      private FilePart fp = this_part;
      private long pos = 0;
      //
      public boolean hasNext() {
        return (pos < fp.getPartSize()) ? true : false;
      }
      public Record next() {
        if (!hasNext()) {
          throw new NoSuchElementException("HPCC file partition at EOF");
        }
        pos += 100;
        ArrayList<FieldContent> test_data = new ArrayList<FieldContent>();
        String field1 = "File part "+ String.valueOf(this_part.getThisPart());
        test_data.add(new StringContent("F1", field1));
        String field2 = "pos " + String.valueOf(pos);
        test_data.add(new StringContent("F2", field2));
        FieldContent[] content = test_data.toArray(new FieldContent[0]);
        Record rslt = new Record(content);
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
