package org.hpccsystems.spark;

import org.hpccsystems.spark.data.HpccSrcType;

public class FieldDefTest {

  public static void main(String[] args) {
    FieldDef f1 = new FieldDef("f1", FieldType.INTEGER, "ty1",
        4, 0, true, HpccSrcType.UNKNOWN, new FieldDef[0]);
    FieldDef f2 = new FieldDef("f2", FieldType.STRING, "ty2",
        0, 0, false, HpccSrcType.UNKNOWN, new FieldDef[0]);
    FieldDef[] s = new FieldDef[2];
    s[0] = f1;
    s[1] = f2;
    FieldDef rec = new FieldDef("root", FieldType.RECORD, "",
        0, 0, false, HpccSrcType.UNKNOWN, s);
    System.out.println(rec.toString());
    System.out.println(s[0].toString());
    System.out.println(s[1].toString());
  }

}
