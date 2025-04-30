/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.polaris.benchmarks.parameters

import com.typesafe.config.Config
import com.typesafe.scalalogging.Logger

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

/**
 * Case class to hold the parameters for the WeightedWorkloadOnTreeDataset simulation.
 *
 * @param seed The RNG seed to use
 * @param readers A seq of distrbutions to use for reading tables
 * @param writers A seq of distrbutions to use for writing to tables
 */
case class WeightedWorkloadOnTreeDatasetParameters(
    seed: Int,
    readers: Seq[Distribution],
    writers: Seq[Distribution],
    durationInMinutes: Int) {
  require(readers.nonEmpty || writers.nonEmpty, "At least one reader or writer is required")
  require(durationInMinutes > 0, "Duration in minutes must be positive")
}

object WeightedWorkloadOnTreeDatasetParameters {
  def loadDistributionsList(config: Config, key: String): List[Distribution] = {
    config.getConfigList(key).asScala.toList.map { conf =>
      Distribution(
        count = conf.getInt("count"),
        mean = conf.getDouble("mean"),
        variance = conf.getDouble("variance")
      )
    }
  }
}

case class Distribution(count: Int, mean: Double, variance: Double) {
  /**
   * Return a value in [0, items) based on this distribution using truncated normal resampling.
   */
  def sample(items: Int, randomNumberProvider: RandomNumberProvider): Int = {
    val stddev = math.sqrt(variance)
    // Resample until the value is in [0, 1]
    val value = Iterator
      .continually(randomNumberProvider.next() * stddev + mean)
      .dropWhile(x => x < 0.0 || x > 1.0)
      .next()

    (value * items).toInt.min(items - 1)
  }
}

object Distribution {

  // Map an index back to a table path
  def tableIndexToIdentifier(index: Int, dp: DatasetParameters): (String, List[String], String) = {
    require(dp.numTablesMax == -1, "Sampling is incompatible with numTablesMax settings other than -1")

    val tablesPerCatalog = dp.totalTables / dp.numCatalogs
    val catalogIndex = index / tablesPerCatalog

    val namespaceOrdinal = dp.nAryTree.pathToRoot(index / dp.numTablesPerNs)
    (s"C_$catalogIndex", namespaceOrdinal.map(n => s"NS_${n}"), s"T_${index}")
  }
}

case class RandomNumberProvider(seed: Int, threadId: Int) {
  private[this] val random = new Random(seed + threadId)
  def next(): Double = random.nextGaussian()
}
