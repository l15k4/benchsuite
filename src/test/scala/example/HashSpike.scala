package example

import java.io.File
import java.nio.charset.StandardCharsets
import java.util.concurrent.Executors

import akka.actor.ActorSystem
import akka.stream._
import akka.stream.scaladsl.{FileIO, Framing}
import akka.util.ByteString
import com.google.common.hash.Hashing
import net.openhft.hashing.LongHashFunction
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.mutable
import scala.concurrent.duration._
import scala.concurrent.{Await, ExecutionContext, Future}

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

  val totalSize = args match {
    case Array(size) =>
      size.toInt
    case _ =>
      system.terminate()
      throw new IllegalArgumentException("Specify number of uuids in a sample !!!")
  }

  val rootDir = "/tmp/HashSpike"

  for {
   _ <- Future(spike(s"$rootDir/murmur3_128", totalSize) (line => Hashing.murmur3_128().hashString(line, StandardCharsets.UTF_8).asLong()))
   _ <- Future(spike(s"$rootDir/openhft_64", totalSize) (line => LongHashFunction.murmur_3().hashBytes(line.getBytes)))
   _ <- Future(spike(s"$rootDir/farmHash_64", totalSize) (line => Hashing.farmHashFingerprint64().hashString(line, StandardCharsets.UTF_8).asLong()))
   _ <- Future(spike(s"$rootDir/sipHash_24", totalSize) (line => Hashing.sipHash24().hashString(line, StandardCharsets.UTF_8).asLong()))
  } yield system.terminate()

  def spike(targetDir: String, sampleSize: Int)(hash: String => Long)(implicit m: Materializer) = {

    new File(targetDir).mkdirs()

    val uuidFile = new File(s"$targetDir/uuid.csv")
    val hashFile = new File(s"$targetDir/hash.csv")
    uuidFile.createNewFile()
    hashFile.createNewFile()

    def hashF =
      FileIO.fromPath(uuidFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .map(line => hash(line).toString)
        .grouped(500)
        .buffer(2, OverflowStrategy.backpressure)
        .async
        .runWith(UuidGenerator.lineSink(hashFile.toPath))
        .andThen { case _ => println(s"$hashFile: hash generation finished ...")}

    def softDuplicatesF =
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
        .andThen { case _ => println(s"$targetDir: soft duplicates check finished ...")}

    def hardDuplicatesF(softDuplicates: Set[Long]) =
      FileIO.fromPath(hashFile.toPath)
        .via(Framing.delimiter(ByteString("\n"), maximumFrameLength = 1024))
        .map(_.utf8String)
        .runFold(new PrimitiveKeyOpenHashMap[Long, Int](16384)) {
          case (acc, line) if softDuplicates.contains(line.toLong) =>
            acc.adjust(line.toLong)(_.map(_ + 1).getOrElse(1))
          case (acc, _) =>
            acc
        }.andThen { case _ => println(s"$targetDir: hard duplicates check finished ...")}

    def resultF =
      for {
        _ <- UuidGenerator.uuidF(sampleSize, 0.00001, uuidFile.toPath)
        _ <- hashF
        softDuplicates <- softDuplicatesF
        hardDuplicates <- hardDuplicatesF(softDuplicates)
      } yield hardDuplicates

    val start = System.currentTimeMillis()
    val result = Await.result(resultF, 24.hours)
    val tookMS = System.currentTimeMillis() - start
    val tookS = tookMS / 1000D

    println(s"$targetDir : ${result.count(_._2 > 1)} collisions found in $tookS seconds !!!")
  }
}