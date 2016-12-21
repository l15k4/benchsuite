package example

import java.util.Comparator

/**
  * A simple wrapper over the Java implementation [[TimSort]].
  *
  * The Java implementation is package private, and hence it cannot be called outside package
  * org.apache.spark.util.collection. This is a simple wrapper of it that is available to spark.
  */
class Sorter[K, Buffer](private val s: SortDataFormat[K, Buffer]) {

  private val timSort = new TimSort(s)

  /**
    * Sorts the input buffer within range [lo, hi).
    */
  def sort(a: Buffer, lo: Int, hi: Int, c: Comparator[_ >: K]): Unit = {
    timSort.sort(a, lo, hi, c)
  }
}
