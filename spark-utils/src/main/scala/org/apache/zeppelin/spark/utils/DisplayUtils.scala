package org.apache.zeppelin.spark.utils

import java.lang.StringBuilder

import org.apache.spark.rdd.RDD

import scala.collection.IterableLike

object DisplayUtils {

  implicit def toDisplayFunctions[T <: Product](rdd: RDD[T]): DisplayFunctions[T] = new DisplayFunctions[T](rdd)

  def html(htmlContent: String = "") = s"%html $htmlContent"

  def img64(base64Content: String = "") = s"%img $base64Content"

  def img(url: String) = s"<img src='$url' />"
}

class DisplayFunctions[T <: Product](val rdd: RDD[T]) {
  def displayAsTable(columnLabels: String*): Unit = {
    val providedLabelCount: Int = columnLabels.size
    var maxColumnCount:Int = 1
    val headers = new StringBuilder("%table ")

    val data = new StringBuilder("")
    rdd.collect().foreach(tuple => {
      maxColumnCount = math.max(maxColumnCount,tuple.productArity)
      data.append(tuple.productIterator.mkString("\t")).append("\n")
    })


    if (providedLabelCount > maxColumnCount) {
      headers.append(columnLabels.take(maxColumnCount).mkString("\t")).append("\n")
    } else if (providedLabelCount < maxColumnCount) {
      val missingColumnHeaders = ((providedLabelCount+1) to maxColumnCount).foldLeft[String](""){
        (stringAccumulator,index) => s"$stringAccumulator\tColumn$index"
      }

      headers.append(columnLabels.mkString("\t")).append(missingColumnHeaders).append("\n")
    } else {
      headers.append(columnLabels.mkString("\t")).append("\n")
    }

    headers.append(data)

    print(headers.toString)
  }


}


