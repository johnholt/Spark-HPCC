package org.hpccsystems.spark;

import java.util.ArrayList;

public class RecordTest {

  public static void main(String[] args) {
    ArrayList<FieldContent> test_data = new ArrayList<FieldContent>();
    String field1 = "File part x";
    test_data.add(new StringContent("F1", field1));
    String field2 = "pos y";
    test_data.add(new StringContent("F2", field2));
    FieldContent[] content = test_data.toArray(new FieldContent[0]);
    Record rec = new Record(content);
    FieldContent[] contents = rec.getFields();
    System.out.println("Number of fields: " + contents.length);
    for (FieldContent fc : contents) {
      System.out.println(fc.getName() + "=" + fc.asString());
    }
  }

}
