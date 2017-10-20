package org.hpccsystems.spark

class Record(
    names : Array[String],
    val content : Array[Any]
    ) extends Serializable{
  val name_tab = new scala.collection.mutable.HashMap[String, Any]()
  for (i <- 0 to names.size-1) {
    name_tab.put(names(i), content(i))
  }
  def get(i : Int) : Any = {
    content(i)
  }
  def getByName(name : String) : Any = {
    name_tab.get(name)
  }
}