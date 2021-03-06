/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// scalastyle:off println
package org.apache.spark.examples

import org.apache.spark._

/** Computes an approximation to pi */
object SparkWordCount {
  def main(args: Array[String]) {
    val conf = new SparkConf().setAppName("Spark WordCount")
    val sc = new SparkContext(conf)

    val textFile = sc.textFile("/home/jan/data/enwikisource-20150901-pages-meta-history.xml")
    val counts = textFile.flatMap(line => line.split(" "))
                 .map(word => (word, 1))
                 .reduceByKey(_ + _)

    val startTime = System.nanoTime()
    counts.saveAsTextFile("/home/jan/data/count.txt")
    val endTime = System.nanoTime()

    println(endTime - startTime)

    sc.stop()
  }
}
// scalastyle:on println
