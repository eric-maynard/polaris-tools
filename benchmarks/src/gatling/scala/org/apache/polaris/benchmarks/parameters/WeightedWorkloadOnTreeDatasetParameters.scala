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
import org.slf4j.LoggerFactory

import scala.collection.JavaConverters._
import scala.collection.immutable.LazyList
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
    durationInMinutes: Int
) {
  require(readers.nonEmpty || writers.nonEmpty, "At least one reader or writer is required")
  require(durationInMinutes > 0, "Duration in minutes must be positive")
}

object WeightedWorkloadOnTreeDatasetParameters {
  def loadDistributionsList(config: Config, key: String): List[Distribution] =
    config.getConfigList(key).asScala.toList.map { conf =>
      Distribution(
        count = conf.getInt("count"),
        mean = conf.getDouble("mean"),
        variance = conf.getDouble("variance")
      )
    }
}

case class Distribution(count: Int, mean: Double, variance: Double) {
  private val logger = LoggerFactory.getLogger(getClass)

  def printDescription(): Unit = {
    println(s"Summary for ${this}:")

    // On startup, print some metadata about the distribution
    printVisualization()

    // Warn if a large amount of resampling will be needed
    val debugRandomNumberProvider = RandomNumberProvider(1, 2)
    def resampleStream: LazyList[Double] =
      LazyList.continually(debugRandomNumberProvider.next() * math.sqrt(variance) + mean)

    val (_, resamples) = resampleStream.zipWithIndex
      .take(100000)
      .find { case (value, _) => value >= 0 && value <= 1 }
      .map { case (value, index) => (value, index) }
      .getOrElse((-1, 100000))

    if (resamples > 5) {
      logger.warn(s"A distribution appears to require aggressive resampling: ${this} took ${resamples + 1} samples!")
    }
  }

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

  def printVisualization(samples: Int = 100000, bins: Int = 10): Unit = {
    val binCounts = Array.fill(bins)(0)
    val rng = RandomNumberProvider("visualization".hashCode, -1)

    for (_ <- 1 to samples) {
      val value = Iterator
        .continually(rng.next() * math.sqrt(variance) + mean)
        .dropWhile(x => x < 0.0 || x > 1.0)
        .next()

      val bin = ((value * bins).toInt).min(bins - 1)
      binCounts(bin) += 1
    }

    val maxBarWidth = 50
    val total = binCounts.sum.toDouble
    println("  Range         | % of Samples | Visualization")
    println("  --------------|--------------|------------------")

    for (i <- 0 until bins) {
      val low = i.toDouble / bins
      val high = (i + 1).toDouble / bins
      val percent = binCounts(i) / total * 100
      val bar = "█" * ((percent / 100 * maxBarWidth).round.toInt)
      println(f"  [$low%.1f - $high%.1f) | $percent%6.2f%%      | $bar")
    }
    println()
  }
}

object Distribution {

  // Map an index back to a table path
  def tableIndexToIdentifier(index: Int, dp: DatasetParameters): (String, List[String], String) = {
    require(
      dp.numTablesMax == -1,
      "Sampling is incompatible with numTablesMax settings other than -1"
    )

    val namespaceIndex = index / dp.numTablesPerNs
    val namespaceOrdinal = dp.nAryTree.lastLevelOrdinals.toList.apply(namespaceIndex)
    val namespacePath = dp.nAryTree.pathToRoot(namespaceOrdinal)

    (s"C_0", namespacePath.map(n => s"NS_${n}"), s"T_${index}")
  }
}

case class RandomNumberProvider(seed: Int, threadId: Int) {
  private[this] val random = new Random(seed + threadId)
  def next(): Double = random.nextGaussian()
}
