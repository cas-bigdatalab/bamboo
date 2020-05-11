package cn.pandadb.costore.utils

import scala.collection.immutable.TreeMap
import util.hashing.MurmurHash3.stringHash

class ConsistentHashRing(val identities: List[String]) {
  val ring = TreeMap(identities.map(id => (stringHash(id) -> id)): _*)

  def getHolders(beHeldID: String, size: Int): List[String] = {
    var itr = ring.iteratorFrom(stringHash(beHeldID))
    List(1 to  size).map(s => {
      itr = if(itr.hasNext) itr else ring.toIterator
      itr.next()._2
    })
  }

}