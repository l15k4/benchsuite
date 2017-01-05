package example

import java.nio.file.Path
import java.util.UUID

import akka.stream.{Attributes, _}
import akka.stream.scaladsl.{FileIO, Flow, Keep, Sink, Source}
import akka.stream.stage.{GraphStage, GraphStageLogic, InHandler, OutHandler}
import akka.util.ByteString
import org.apache.spark.util.sketch.BloomFilter

import scala.collection.immutable
import scala.concurrent.{ExecutionContext, Future}

object UuidGenerator {

  def lineSink(file: Path): Sink[immutable.Seq[String], Future[IOResult]] =
    Flow[immutable.Seq[String]]
      .map(records => ByteString(records.mkString("", "\n", "\n")))
      .toMat(FileIO.toPath(file))(Keep.right)

  def bfStage(sampleSize: Int, fpp: Double) =
    new BFilter[String](BloomFilter.create(sampleSize, fpp), {
      (bf, uuid) =>
        val contains = bf.mightContainString(uuid)
        if (!contains) bf.putString(uuid)
        !contains
    }
    )

  def uuidF(sampleSize: Int, fpp: Double, uuidFilePath: Path)(implicit m: Materializer) =
    Source.fromIterator(() => Iterator.range(0, sampleSize))
      .map(_ => UUID.randomUUID().toString)
      .via(bfStage(sampleSize, fpp))
      .grouped(500)
      .buffer(2, OverflowStrategy.backpressure)
      .async
      .runWith(lineSink(uuidFilePath))
      .andThen { case _ => println(s"$uuidFilePath: UUID generation finished ...")}(ExecutionContext.Implicits.global)

  class BFilter[A](bf: BloomFilter, p: (BloomFilter, A) => Boolean) extends GraphStage[FlowShape[A, A]] {
    val in = Inlet[A]("Filter.in")
    val out = Outlet[A]("Filter.out")
    val shape = FlowShape.of(in, out)
    override def createLogic(inheritedAttributes: Attributes): GraphStageLogic =
      new GraphStageLogic(shape) {
        setHandler(in, new InHandler {
          override def onPush(): Unit = {
            val elem = grab(in)
            if (p(bf, elem)) push(out, elem)
            else pull(in)
          }
        })
        setHandler(out, new OutHandler {
          override def onPull(): Unit = {
            pull(in)
          }
        })
      }
  }

}