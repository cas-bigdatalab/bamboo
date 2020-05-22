package cn.pandadb.costore.utils

import scala.collection.immutable.TreeMap
import util.hashing.MurmurHash3.stringHash

class ConsistentHashRing(val identities: List[String]) {
  val ring = TreeMap(identities.map(id => (stringHash(id) -> id)): _*)

  def getHolder(beHeldID: String, size: Int): String = {
    var itr = ring.iteratorFrom(stringHash(beHeldID))
    itr = if(itr.hasNext) itr else ring.iterator
    itr.next()._2
  }

}
