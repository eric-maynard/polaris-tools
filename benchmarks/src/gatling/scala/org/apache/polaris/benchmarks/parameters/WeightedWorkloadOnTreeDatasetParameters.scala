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

import scala.collection.JavaConverters._

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

case class Distribution(count: Int, mean: Double, variance: Double)
