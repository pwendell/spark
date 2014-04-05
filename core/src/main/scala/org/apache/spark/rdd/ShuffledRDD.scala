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

package org.apache.spark.rdd

import scala.reflect.ClassTag

import org.apache.spark.{Dependency, Partition, Partitioner, ShuffleDependency, SparkEnv, TaskContext}
import org.apache.spark.serializer.Serializer

private[spark] class ShuffledRDDPartition(val idx: Int) extends Partition {
  override val index = idx
  override def hashCode(): Int = idx
}

/**
 * <span class="badge" style="float: right; background-color: #44751E;">DEVELOPER API</span>
 * The resulting RDD from a shuffle (e.g. repartitioning of data).
 * @param prev the parent RDD.
 * @param part the partitioner used to partition the RDD
 * @tparam K the key class.
 * @tparam V the value class.
 */
class ShuffledRDD[K, V, P <: Product2[K, V] : ClassTag](
    @transient var prev: RDD[P],
    part: Partitioner)
  extends RDD[P](prev.context, Nil) {

  private var serializer: Serializer = null

  def setSerializer(serializer: Serializer): ShuffledRDD[K, V, P] = {
    this.serializer = serializer
    this
  }

  override def getDependencies: Seq[Dependency[_]] = {
    List(new ShuffleDependency(prev, part, serializer))
  }

  override val partitioner = Some(part)

  override def getPartitions: Array[Partition] = {
    Array.tabulate[Partition](part.numPartitions)(i => new ShuffledRDDPartition(i))
  }

  override def compute(split: Partition, context: TaskContext): Iterator[P] = {
    val shuffledId = dependencies.head.asInstanceOf[ShuffleDependency[K, V]].shuffleId
    val ser = Serializer.getSerializer(serializer)
    SparkEnv.get.shuffleFetcher.fetch[P](shuffledId, split.index, context, ser)
  }

  override def clearDependencies() {
    super.clearDependencies()
    prev = null
  }
}
