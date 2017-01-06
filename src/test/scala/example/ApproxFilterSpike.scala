package example

import java.io.{ByteArrayOutputStream, File}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}
import java.util.concurrent.Executors

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Framing, Sink}
import akka.util.ByteString
import com.github.mgunlogson.cuckoofilter4j.CuckooFilter
import com.github.mgunlogson.cuckoofilter4j.Utils.Algorithm
import com.google.common.hash
import com.google.common.hash.Funnels
import org.apache.spark.util.sketch

import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object ApproxFilterSpike extends App {

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  implicit val system = ActorSystem("ApproxFilterSpike")
  implicit val materializer = ActorMaterializer()

  val (sampleSize, fpp) = args match { // note that fpp = 0.00001 offers the lowest collision rate and size for UUIDs
    case Array(size, falsePositiveRate) =>
      size.toInt -> falsePositiveRate.toDouble
    case _ =>
      system.terminate()
      throw new IllegalArgumentException("Specify number of uuids in a sample !!!")
  }

  val targetDir = "/tmp/ApproxFilterSpike"
  new File(targetDir).mkdirs()
  val uuidFile = new File(s"$targetDir/uuid.csv")
  uuidFile.createNewFile()

  val sparkBloomFilterSink: Sink[String, Future[(Int, sketch.BloomFilter)]] =
    Sink.fold(0 -> sketch.BloomFilter.create(sampleSize, fpp)) {
      case ((count, bf), uuid) if bf.mightContainString(uuid) =>
        bf.putString(uuid)
        count+1 -> bf
      case ((count, bf), uuid) =>
        bf.putString(uuid)
        count -> bf
    }

  val guavaBloomFilterSink: Sink[String, Future[(Int, hash.BloomFilter[CharSequence])]] =
    Sink.fold(0 -> hash.BloomFilter.create[CharSequence](Funnels.stringFunnel(StandardCharsets.UTF_8), sampleSize, fpp)) {
      case ((count, bf), uuid) if bf.mightContain(uuid) =>
        bf.put(uuid)
        count+1 -> bf
      case ((count, bf), uuid) =>
        bf.put(uuid)
        count -> bf
    }

  def cuckooFilterSink(algorithm: Algorithm): Sink[String, Future[(Int, CuckooFilter[String])]] = {
    val cuckooF = new CuckooFilter.Builder[String](Funnels.stringFunnel(StandardCharsets.UTF_8), sampleSize)
        .withFalsePositiveRate(fpp)
        .withHashAlgorithm(algorithm)
        .withExpectedConcurrency(4)
        .build()
    Sink.fold(0 -> cuckooF) {
      case ((count, bf), uuid) if bf.mightContain(uuid) =>
        bf.put(uuid)
        count+1 -> bf
      case ((count, bf), uuid) =>
        bf.put(uuid)
        count -> bf
    }
  }

  def writeFilterToFile(name: String, fn: ByteArrayOutputStream => Unit): Long = {
    val out = new ByteArrayOutputStream()
    try fn(out) finally out.close()
    val bfBytes = out.toByteArray
    Files.write(Paths.get(targetDir, name), bfBytes).toFile.length()
  }

  def getSparkFilterSize(bf: sketch.BloomFilter) = writeFilterToFile("sparkBF.bf", out => bf.writeTo(out))
  def getGuavaFilterSize(bf: hash.BloomFilter[CharSequence]) = writeFilterToFile("guavaBF.bf", out => bf.writeTo(out))
  def getCuckooFilterSize(bf: CuckooFilter[String]) = bf.getStorageSize / 8

  def spike[S](name: String, bfSink: Sink[String, Future[(Int, S)]])(getSize: S => Long): Future[(Int, S)] = {
    def collisionCountF: Future[(Int, S)] =
      FileIO.fromPath(uuidFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .runWith(bfSink)

    val start = System.currentTimeMillis()

    collisionCountF.andThen {
      case Success((count, filter)) =>
        def took = (System.currentTimeMillis() - start) / 1000D
        println(s"$name finished in $took seconds with $count collisions and size ${getSize(filter)} bytes ...")
    }
  }

  val futureResult =
    for {
      _ <- UuidGenerator.writeUuids(sampleSize, 0.00001, uuidFile.toPath)
      _ <- spike[sketch.BloomFilter]("sparkBF", sparkBloomFilterSink)(getSparkFilterSize)
      _ <- spike[hash.BloomFilter[CharSequence]]("guavaBF", guavaBloomFilterSink)(getGuavaFilterSize)
      _ <- spike[CuckooFilter[String]]("cuckooF sipHash24", cuckooFilterSink(Algorithm.sipHash24))(getCuckooFilterSize)
      _ <- spike[CuckooFilter[String]]("cuckooF Murmur3_128", cuckooFilterSink(Algorithm.Murmur3_128))(getCuckooFilterSize)
      _ <- spike[CuckooFilter[String]]("cuckooF sha256", cuckooFilterSink(Algorithm.sha256))(getCuckooFilterSize)
    } yield Done

  futureResult onComplete {
    case Success(_) =>
      system.terminate() andThen { case _ => System.exit(0) }
    case Failure(ex) =>
      println(ex)
      system.terminate() andThen { case _ => System.exit(0) }
  }

}
