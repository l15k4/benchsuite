package example

import com.twitter.algebird.HyperLogLog.int2Bytes
import com.twitter.algebird.HyperLogLogMonoid

object HyperLogLogBench extends App {

  val size = args(0).toInt

  val hllMonoid = new HyperLogLogMonoid(bits = 8)
  var combinedHLL = hllMonoid.zero
  Iterator.from(0).takeWhile(_ < size).foreach { index =>
    combinedHLL = combinedHLL + hllMonoid.create(index)
  }

}
