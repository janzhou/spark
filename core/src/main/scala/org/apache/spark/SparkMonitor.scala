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

package org.apache.spark

import org.apache.spark.rdd
import org.apache.spark.rdd.RDD
import org.apache.spark.scheduler.TaskLocation
import org.apache.spark.storage.{BlockManagerId, BlockManager, BlockId, RDDBlockId}

import scala.collection.mutable.HashSet

object SparkMonitor extends Logging {
  private def getBlockIds(rdd: RDD[_]): Array[BlockId] = {
    rdd.partitions.indices.map(index => RDDBlockId(rdd.id, index)).toArray[BlockId]
  }

  private def getBlockManagerId(rdd: RDD[_]): Map[BlockId, Seq[BlockManagerId]] = {
    val blockIds = getBlockIds(rdd)
    BlockManager.blockIdsToBlockManagers(
      blockIds, rdd.sparkContext.env, rdd.sparkContext.env.blockManager.master
    )
  }

  def showBlocks(rdd: RDD[_]) = {
    getBlockIds(rdd).foreach { id =>
      val status = rdd.sparkContext.env.blockManager.getStatus(id)
      if ( status.isDefined ) {
        println(s"block: %s, cached %d, mem %l, disk %l".format(
          id.toString, status.get.isCached, status.get.memSize, status.get.diskSize
        ))
      } else {
        println(s"block: %s".format(
          id.toString
        ))
      }
    }
  }

  def showCacheLocs(rdd: RDD[_]) = {
    val blockIds = getBlockIds(rdd)
    val bmIds = getBlockManagerId(rdd)

    blockIds.foreach { id =>
      bmIds.getOrElse(id, Nil).foreach { bm =>
        println("block %s, host: %s, executor: %s".format(id.name, bm.host, bm.executorId))
      }
    }
  }

  def showLocs(rdd: RDD[_]) = {
    rdd.partitions.foreach { p =>
      val prefLocs =  rdd.sparkContext.dagScheduler.getPreferredLocs(rdd, p.index)
      var output = "partition %d: ".format(p.index)
      prefLocs.foreach(output += _.host + " " )
      println(output)
    }
  }
}


