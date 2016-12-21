package example

import cats.implicits._
import com.google.common.hash.Hashing
import com.rklaehn.abc._
import com.rklaehn.sonicreducer.Reducer
import org.scalameter.KeyValue
import org.scalameter.api._

class CollectionsBench extends Bench.OnlineRegressionReport {

  override def measurer = new Executor.Measurer.Default

  def arr = Iterator.from(0).map { index =>
    (Hashing.murmur3_32().hashInt(index).asInt(), index)
  }

  val execs = Seq[KeyValue](
    exec.benchRuns -> 2,
    exec.maxWarmupRuns -> 1,
    exec.independentSamples -> 1,
    exec.requireGC -> true,
    exec.jvmflags -> List("-server", "-Xms2048m", "-Xmx8048m", "-XX:+UseG1GC")
  )

  val gen = Gen.range("size")(5000000, 15000000, 5000000)

  performance of "OpenHashMap" in {
    def buildOpenMap(size: Int): PrimitiveKeyOpenHashMap[Int, Int] =
      arr.take(size).foldLeft(new PrimitiveKeyOpenHashMap[Int, Int](size)) { case (acc, (k,v)) =>
        acc.update(k,v)
        acc
      }
    performance of "build" in {
      using(gen)
        .config(execs: _*)
        .in { size =>
          buildOpenMap(size)
        }
    }
    performance of "get" in {
      using(gen.map(buildOpenMap))
        .config(execs: _*)
        .in { hashMap =>
          val size = hashMap.size
          arr.take(size).foreach { case (k,v) =>
            assert(hashMap(k) == v)
          }
        }
    }
  }

  performance of "ArrayMap" in {
    def buildArrayMap(size: Int) = {
      val reducer = Reducer[ArrayMap[Int, Int]](_ merge _)
      for((k,v) <- arr.take(size))
        reducer(ArrayMap.singleton(k, v))
      reducer.result
    }
    performance of "build" in {
      using(gen)
        .config(execs: _*)
        .in { size =>
          buildArrayMap(size)
        }
    }
    performance of "get" in {
      using(gen.map(buildArrayMap))
        .config(execs: _*)
        .in { hashMap =>
          val size = hashMap.size
          arr.take(size).foreach { case (k,v) =>
            assert(hashMap.get(k).contains(v))
          }
        }
    }

  }

}
