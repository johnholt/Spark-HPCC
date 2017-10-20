package org.hpccsystems.spark

import org.hpccsystems.ws.client.platform.DFUFileDetailInfo
import scala.collection.mutable.ArrayBuffer;


abstract class FieldDef(val fieldName : String) extends Serializable {
  def toString : String;
}

object FieldType extends Enumeration {
  val Int, Str, Real, Dataset, Struct, Error = Value;
  def asText(t : FieldType.Value) : String = {
    t match {
      case FieldType.Int => "Integer"
      case FieldType.Str => "String"
      case FieldType.Real => "Real"
      case FieldType.Dataset => "Dataset"
      case FieldType.Struct => "Structure"
      case FieldType.Error => "Error"
      case _ => "Unknown"
    }
  }
}

class SimpleField(name : String, val fieldType : FieldType.Value)
     extends FieldDef(name) {
  override def toString = {
    "Type " + FieldType.asText(fieldType) + " named " + fieldName + "\n";
  }
}

class StructField(name : String, val fieldList : Array[FieldDef])
     extends FieldDef(name) {
  override def toString = {
    var sb = new StringBuilder
    sb.append("Structure ")
    sb.append(fieldName)
    sb.append(" begin\n")
    for (fld <- fieldList) sb.append(fld.toString()).append(";")
    sb.append("\nEnd of ").append(fieldName).append("\n")
    sb.toString()
  }
}

class ChildDataset(fieldName : String, val structType : StructField)
     extends FieldDef(fieldName) {
  override def toString = {
    "Not yet implemented"
  }
}

object FieldDef {
  def define_record(dfu_info : DFUFileDetailInfo) : FieldDef = {
    var fld_list = dfu_info.getColumns();
    val struct_def = new ArrayBuffer[FieldDef]();
    val IntPattern = "(UNSIGNED|INTEGER)[1-8]?".r
    val StrPattern = "(STRING|UNICODE)[0-9]*".r
    val RealPattern = "REAL([1-8])?".r
    val DSPattern = "TABLE".r
    for (i <- 0 to fld_list.size()-1) {
      var col_type = fld_list.get(i).getColumnEclType.toUpperCase
      var fld_name = fld_list.get(i).getColumnLabel.toLowerCase
      val this_item = col_type match {
        case IntPattern(_)  => new SimpleField(fld_name, FieldType.Int)
        case StrPattern(_)  => new SimpleField(fld_name, FieldType.Str)
        case RealPattern(_) => new SimpleField(fld_name, FieldType.Real)
        case _ => new SimpleField(fld_name, FieldType.Error)
      }
      println(fld_name + " (" + col_type + ") = " + this_item.toString)
      struct_def += this_item
    }
    val rslt : StructField = new StructField("<Root>", struct_def.toArray);
    rslt
  }

}
