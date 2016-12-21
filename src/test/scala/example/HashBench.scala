package example

import com.google.common.hash.Hashing
import net.openhft.hashing.LongHashFunction
import org.scalameter.Bench.{ForkedTime, OfflineReport}
import org.scalameter.KeyValue
import org.scalameter.api._

import scala.reflect.ClassTag

object HashBenchGroup extends Bench.Group {
  performance of "memory footprint" config(reports.resultDir -> "target/benchmarks/hash/memory") in {
    include(new HashMemoryBench {})
  }
  performance of "running time" config(reports.resultDir -> "target/benchmarks/hash/time") in {
    include(new HashPerfBench {})
  }
}

trait HashMemoryBench extends ForkedTime with HashBench[Double] {
  override def measurer = new Executor.Measurer.MemoryFootprint
}

trait HashPerfBench extends OfflineReport with HashBench[Double] {
  override def measurer = new Executor.Measurer.Default

  performance of "get" in {
    performance of "get murmur3_32" in {
      using(gen.map(i => i -> buildOpenSet(i, murmur3_32_Stream)))
        .config(execs: _*)
        .in { case (size, set) =>
          assert(set.size == size)
          murmur3_32_Stream.take(size).foreach(i => assert(set.contains(i)))
        }
    }
    performance of "get murmur3_64" in {
      using(gen.map(i => i -> buildOpenSet(i, murmur3_64_Stream)))
        .config(execs: _*)
        .in { case (size, set) =>
          assert(set.size == size)
          murmur3_64_Stream.take(size).foreach(i => assert(set.contains(i)))
        }
    }
  }
}


trait HashBench[U] extends Bench[U] {

  def murmur3_32_Stream = Iterator.from(0).map( i => Hashing.murmur3_32().hashInt(Hashing.murmur3_32().hashInt(i).asInt()).asInt() )
  def murmur3_64_Stream = Iterator.from(0).map( i => LongHashFunction.murmur_3().hashLong(LongHashFunction.murmur_3().hashInt(i)) )

  val execs = Seq[KeyValue](
    exec.benchRuns -> 2,
    exec.maxWarmupRuns -> 1,
    exec.independentSamples -> 1,
    exec.requireGC -> true,
    exec.jvmflags -> List("-server", "-Xms2048m", "-Xmx16g", "-XX:+UseG1GC")
  )

  val gen = Gen.range("size")(50000000, 100000000, 50000000)

  def buildOpenSet[@specialized(Long, Int) T: ClassTag](size: Int, stream: Iterator[T]) =
    stream.take(size).foldLeft(new OpenHashSet[T](size)) { case (acc, hash) =>
      acc.add(hash)
      acc
    }

  performance of "OpenHashMap" in {
    performance of "build" in {
      performance of "murmur3_32" in {
        using(gen)
          .config(execs: _*)
          .in { size =>
            buildOpenSet(size, murmur3_32_Stream)
          }
      }
      performance of "murmur3_64_Stream" in {
        using(gen)
          .config(execs: _*)
          .in { size =>
            buildOpenSet(size, murmur3_64_Stream)
          }
      }
    }
  }

}
