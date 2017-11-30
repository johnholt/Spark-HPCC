package org.hpccsystems.spark;

public class TypeDef {
  private FieldType type;
  private String typeName;
  private int lenData;
  private FieldDef[] struct;
  private boolean unsignedFlag;
  // flag values from eclhelper.hpp RtlFieldTypeMask enum definition
  final private short flag_unsigned = 256;
  final private short flag_unknownsize = 1024;
  // type codes from rtlconst.hpp type_vals enum definition
  final private short type_boolean = 0;
  final private short type_int = 1;
  final private short type_real = 2;
  final private short type_string = 4;
  final private short type_record = 13;
  final private short type_varstring = 14;
  final private short type_table = 20;
  final private short type_set = 21;
  final private short type_unicode = 31;
  final private short type_varunicode = 33;
  final private short type_utf8 = 41;
  final private short type_uint = flag_unsigned + type_int;

  public TypeDef(short type, short flag, String typeName, int len) {
    this.typeName = typeName;
    this.lenData = len;
    this.struct = new FieldDef[0];
    this.unsignedFlag = type==type_uint;
    switch (type) {
      case type_boolean:
        this.type = FieldType.BOOLEAN;
        break;
      case type_int:
      case type_uint:
        this.type = FieldType.INTEGER;
        break;
      default:
        this.type = FieldType.MISSING;
    }
  }

}
