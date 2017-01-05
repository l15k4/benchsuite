package example

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import akka.Done
import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString
import com.google.common.hash.Hashing
import net.openhft.hashing.LongHashFunction
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable
import scala.concurrent.{ExecutionContext, Future}
import scala.util.{Failure, Success}

object HashSpike extends App {

  implicit class ImpressionsCountPimp(underlying: PrimitiveKeyOpenHashMap[Long, Int]) {
    def adjust(k: Long)(f: Option[Int] => Int): PrimitiveKeyOpenHashMap[Long, Int] = {
      underlying.update(k, f(Option(underlying.getOrElse(k, null.asInstanceOf[Int]))))
      underlying
    }
  }

  implicit val ec = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))
  implicit val system = ActorSystem("HashSpike")
  implicit val materializer = ActorMaterializer()

  val rootDir = "/tmp/HashSpike"
  val uuidFile = new File(s"$rootDir/uuid.csv")
  uuidFile.createNewFile()

  val sampleSize = args match {
    case Array(size) =>
      size.toInt
    case _ =>
      system.terminate()
      throw new IllegalArgumentException("Specify number of uuids in a sample !!!")
  }

  val result =
    for {
      _ <- UuidGenerator.uuidF(sampleSize, 0.00001, uuidFile.toPath)
      _ <- spike(s"$rootDir/murmur3_128", sampleSize) (line => Hashing.murmur3_128().hashString(line, StandardCharsets.UTF_8).asLong())
      _ <- spike(s"$rootDir/openhft_64", sampleSize) (line => LongHashFunction.murmur_3().hashBytes(line.getBytes))
      _ <- spike(s"$rootDir/farmHash_64", sampleSize) (line => Hashing.farmHashFingerprint64().hashString(line, StandardCharsets.UTF_8).asLong())
      _ <- spike(s"$rootDir/sipHash_24", sampleSize) (line => Hashing.sipHash24().hashString(line, StandardCharsets.UTF_8).asLong())
    } yield Done

  result onComplete {
    case Success(_) =>
      system.terminate() andThen { case _ => System.exit(0) }
    case Failure(ex) =>
      println(ex)
      system.terminate() andThen { case _ => System.exit(0) }
  }

  def spike(targetDir: String, sampleSize: Int)(hash: String => Long)(implicit m: Materializer): Future[Int] = {

    new File(targetDir).mkdirs()
    val hashFile = new File(s"$targetDir/hash.csv")
    hashFile.createNewFile()

    def generateHashCodesToFile =
      FileIO.fromPath(uuidFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .map(line => hash(line).toString)
        .grouped(500)
        .buffer(2, OverflowStrategy.backpressure)
        .async
        .runWith(UuidGenerator.lineSink(hashFile.toPath))
        .andThen { case _ => println(s"$hashFile: hash code generation finished ...")}

    def findApproxDuplicates =
      FileIO.fromPath(hashFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .runFold((mutable.HashSet.empty[Long], BloomFilter.create(sampleSize, 0.00001))) {
          case ((acc, bf), line) if bf.mightContainLong(line.toLong) =>
            (acc += line.toLong, bf)
          case ((acc, bf), line) =>
            bf.putLong(line.toLong)
            acc -> bf
        }.map(_._1.toSet)
        .andThen { case _ => println(s"$targetDir: approx duplicates check finished ...")}

    def findDuplicates(approxDuplicates: Set[Long]) =
      FileIO.fromPath(hashFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .runFold(new PrimitiveKeyOpenHashMap[Long, Int](16384)) {
          case (acc, line) if approxDuplicates.contains(line.toLong) =>
            acc.adjust(line.toLong)(_.map(_ + 1).getOrElse(1))
          case (acc, _) =>
            acc
        }.andThen { case _ => println(s"$targetDir: duplicates check finished ...")}

    def resultF =
      for {
        _ <- generateHashCodesToFile
        approxDuplicates <- findApproxDuplicates
        duplicates <- findDuplicates(approxDuplicates)
      } yield duplicates.count(_._2 > 1)

    val start = System.currentTimeMillis()
    def ended = (System.currentTimeMillis() - start) / 1000D

    resultF.andThen {
      case Success(duplicatesCount) =>
        println(s"$targetDir : $duplicatesCount collisions found in $ended seconds !!!")
    }

  }
}