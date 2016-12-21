package example
import scala.reflect.ClassTag

/**
  * An append-only buffer that keeps track of its estimated size in bytes.
  */
class SizeTrackingVector[T: ClassTag]
  extends PrimitiveVector[T]
    with SizeTracker {

  override def +=(value: T): Unit = {
    super.+=(value)
    super.afterUpdate()
  }

  override def resize(newLength: Int): PrimitiveVector[T] = {
    super.resize(newLength)
    resetSamples()
    this
  }
}