package org.hpccsystems.spark;

import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import org.apache.spark.sql.Row;
import org.apache.spark.sql.types.StructType;

import scala.collection.Seq;

public class HpccRow implements Row, Serializable {

  public HpccRow() {
    // TODO Auto-generated constructor stub
  }

  @Override
  public boolean anyNull() {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public Object apply(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Row copy() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int fieldIndex(String arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Object get(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getAs(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> T getAs(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean getBoolean(int arg0) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public byte getByte(int arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Date getDate(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public BigDecimal getDecimal(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public double getDouble(int arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public float getFloat(int arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int getInt(int arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public <K, V> Map<K, V> getJavaMap(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> List<T> getList(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public long getLong(int arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public <K, V> scala.collection.Map<K, V> getMap(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> Seq<T> getSeq(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public short getShort(int arg0) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String getString(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Row getStruct(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public Timestamp getTimestamp(int arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public <T> scala.collection.immutable.Map<String, T> getValuesMap(Seq<String> arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public boolean isNullAt(int arg0) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public int length() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public String mkString() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String mkString(String arg0) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public String mkString(String arg0, String arg1, String arg2) {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public StructType schema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public int size() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public Seq<Object> toSeq() {
    // TODO Auto-generated method stub
    return null;
  }

}
