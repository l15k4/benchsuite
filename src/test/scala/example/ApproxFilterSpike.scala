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

  val (sampleSize, fpp) = args match {
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

  val sparkBF: Sink[String, Future[(Int, sketch.BloomFilter)]] =
    Sink.fold(0 -> sketch.BloomFilter.create(sampleSize, fpp)) {
      case ((count, bf), uuid) if bf.mightContainString(uuid) =>
        bf.putString(uuid)
        count+1 -> bf
      case ((count, bf), uuid) =>
        bf.putString(uuid)
        count -> bf
    }

  val guavaBF: Sink[String, Future[(Int, hash.BloomFilter[CharSequence])]] =
    Sink.fold(0 -> hash.BloomFilter.create[CharSequence](Funnels.stringFunnel(StandardCharsets.UTF_8), sampleSize, fpp)) {
      case ((count, bf), uuid) if bf.mightContain(uuid) =>
        bf.put(uuid)
        count+1 -> bf
      case ((count, bf), uuid) =>
        bf.put(uuid)
        count -> bf
    }

  def cuckooBF(algorithm: Algorithm): Sink[String, Future[(Int, CuckooFilter[String])]] = {
    val cuckooF = new CuckooFilter.Builder[String](Funnels.stringFunnel(StandardCharsets.UTF_8), sampleSize)
        .withFalsePositiveRate(fpp)
        .withHashAlgorithm(algorithm)
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

  def writeFile(name: String, fn: ByteArrayOutputStream => Unit): Long = {
    val out = new ByteArrayOutputStream()
    try fn(out) finally out.close()
    val bfBytes = out.toByteArray
    Files.write(Paths.get(targetDir, name), bfBytes).toFile.length()
  }

  def writeSparkBF(bf: sketch.BloomFilter) = writeFile("sparkBF.bf", out => bf.writeTo(out))
  def writeGuavaBF(bf: hash.BloomFilter[CharSequence]) = writeFile("guavaBF.bf", out => bf.writeTo(out))

  val result =
    for {
      _ <- UuidGenerator.uuidF(sampleSize, 0.00001, uuidFile.toPath)
      _ <- spike[sketch.BloomFilter]("sparkBF", sampleSize, uuidFile, sparkBF)(writeSparkBF)
      _ <- spike[hash.BloomFilter[CharSequence]]("guavaBF", sampleSize, uuidFile, guavaBF)(writeGuavaBF)
      _ <- spike[CuckooFilter[String]]("sipHash24 cuckooBF", sampleSize, uuidFile, cuckooBF(Algorithm.sipHash24))(_ => 0)
      _ <- spike[CuckooFilter[String]]("Murmur3_128 cuckooBF", sampleSize, uuidFile, cuckooBF(Algorithm.Murmur3_128))(_ => 0)
      _ <- spike[CuckooFilter[String]]("sha256 cuckooBF", sampleSize, uuidFile, cuckooBF(Algorithm.sha256))(_ => 0)
    } yield Done

  result onComplete {
    case Success(_) =>
      system.terminate() andThen { case _ => System.exit(0) }
    case Failure(ex) =>
      println(ex)
      system.terminate() andThen { case _ => System.exit(0) }
  }

  def spike[S](name: String, sampleSize: Int, uuidFile: File, bfSink: Sink[String, Future[(Int, S)]])(persist: S => Long)(implicit m: Materializer): Future[(Int, S)] = {

    def collisionCountF: Future[(Int, S)] =
      FileIO.fromPath(uuidFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .runWith(bfSink)

    val start = System.currentTimeMillis()
    def ended = (System.currentTimeMillis() - start) / 1000D

    collisionCountF.andThen {
      case Success((count, filter)) =>
        val size = persist(filter)
        println(s"$name finished in $ended seconds with $count collisions and size $size bytes ...")
    }

  }
}
