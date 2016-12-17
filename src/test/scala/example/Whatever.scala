package example

import gnu.trove.map.hash.TLongLongHashMap
import it.unimi.dsi.fastutil.longs.Long2LongOpenHashMap
import net.openhft.hashing.LongHashFunction
import org.scalatest.FreeSpec


class Whatever extends FreeSpec {

  val cnt = 80000000

  "default" in {

    val map = new java.util.HashMap[Long, Long](cnt)
    var index = 0
    while (index < cnt) {
      map.put(LongHashFunction.murmur_3().hashChars(index.toString), 0L)
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map.containsKey(LongHashFunction.murmur_3().hashChars(index.toString)))
    }
  }

  "open" in {

    val map = new OpenHashMap[Long, Long](cnt)
    var index = 0
    while (index < cnt) {
      map(LongHashFunction.murmur_3().hashChars(index.toString)) = 0L
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map.contains(LongHashFunction.murmur_3().hashChars(index.toString)))
    }
  }

  "hppc" in {

    val map = new com.carrotsearch.hppc.LongLongHashMap(cnt)
    var index = 0
    while (index < cnt) {
      map.put(LongHashFunction.murmur_3().hashChars(index.toString), 0L)
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map.containsKey(LongHashFunction.murmur_3().hashChars(index.toString)))
    }

  }

  "gs" in {

    val map = new com.gs.collections.impl.map.mutable.primitive.LongLongHashMap()
    var index = 0
    while (index < cnt) {
      map.put(LongHashFunction.murmur_3().hashChars(index.toString), 0L)
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map.containsKey(LongHashFunction.murmur_3().hashChars(index.toString)))
    }

  }

  "trove" in {

    val map = new TLongLongHashMap()
    var index = 0
    while (index < cnt) {
      map.put(LongHashFunction.murmur_3().hashChars(index.toString), 0L)
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map.containsKey(LongHashFunction.murmur_3().hashChars(index.toString)))
    }

  }

  "mb" in {

    val map = MbHashmap.empty[Long,Long]()
    var index = 0
    while (index < cnt) {
      map.update(LongHashFunction.murmur_3().hashChars(index.toString), 0L)
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map(LongHashFunction.murmur_3().hashChars(index.toString)).isDefined)
    }

  }

  "fu" in {
    val map = new Long2LongOpenHashMap(cnt)
    var index = 0
    while (index < cnt) {
      map.put(LongHashFunction.murmur_3().hashChars(index.toString), 0L)
      index+=1
    }
    println("Checking")
    while (index != 0) {
      index-=1
      assert(map.containsKey(LongHashFunction.murmur_3().hashChars(index.toString)))
    }
  }

}
