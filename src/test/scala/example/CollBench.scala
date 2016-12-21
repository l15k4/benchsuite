package example

import cats.implicits._
import com.google.common.hash.Hashing
import com.rklaehn.abc._
import com.rklaehn.sonicreducer.Reducer
import org.scalameter.Bench.{ForkedTime, OfflineReport}
import org.scalameter.{Bench, KeyValue}
import org.scalameter.api._

object CollBenchGroup extends Bench.Group {
  performance of "memory footprint" config(reports.resultDir -> "target/benchmarks/coll/memory") in {
    include(new HashMemoryBench {})
  }
  performance of "running time" config(reports.resultDir -> "target/benchmarks/coll/time") in {
    include(new HashPerfBench {})
  }
}

trait CollMemoryBench extends ForkedTime with CollBench[Double] {
  override def measurer = new Executor.Measurer.MemoryFootprint
}

trait CollPerfBench extends OfflineReport with CollBench[Double] {
  override def measurer = new Executor.Measurer.Default

  performance of "get" in {
    performance of "OpenHashMap" in {
      using(gen.map(buildOpenMap))
        .config(execs: _*)
        .in { hashMap =>
          val size = hashMap.size
          hashKeyValueStream.take(size).foreach { case (k,v) =>
            assert(hashMap(k) == v)
          }
        }
    }
    performance of "ArrayMap" in {
      using(gen.map(buildArrayMap))
        .config(execs: _*)
        .in { hashMap =>
          val size = hashMap.size
          hashKeyValueStream.take(size).foreach { case (k,v) =>
            assert(hashMap.get(k).contains(v))
          }
        }
    }
  }
}

trait CollBench[U] extends Bench[U] {
  def hashKeyValueStream = Iterator.from(0).map { index =>
    (Hashing.murmur3_32().hashInt(index).asInt(), index)
  }
  def buildOpenMap(size: Int): PrimitiveKeyOpenHashMap[Int, Int] =
    hashKeyValueStream.take(size).foldLeft(new PrimitiveKeyOpenHashMap[Int, Int](size)) { case (acc, (k,v)) =>
      acc.update(k,v)
      acc
    }
  def buildArrayMap(size: Int) = {
    val reducer = Reducer[ArrayMap[Int, Int]](_ merge _)
    for((k,v) <- hashKeyValueStream.take(size))
      reducer(ArrayMap.singleton(k, v))
    reducer.result
  }
  val gen = Gen.range("size")(50000, 150000, 50000)
  val execs = Seq[KeyValue](
    exec.benchRuns -> 2,
    exec.maxWarmupRuns -> 1,
    exec.independentSamples -> 1,
    exec.requireGC -> true,
    exec.jvmflags -> List("-server", "-Xms2048m", "-Xmx16g", "-XX:+UseG1GC")
  )

  performance of "build" in {
    performance of "OpenHashMap" in {
      using(gen)
        .config(execs: _*)
        .in { size =>
          buildOpenMap(size)
        }
    }
    performance of "ArrayMap" in {
      using(gen)
        .config(execs: _*)
        .in { size =>
          buildArrayMap(size)
        }
    }
  }
}
