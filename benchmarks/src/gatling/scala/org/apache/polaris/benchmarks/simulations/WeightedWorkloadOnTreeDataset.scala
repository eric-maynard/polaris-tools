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

package org.apache.polaris.benchmarks.simulations

import io.gatling.core.Predef._
import io.gatling.core.structure.{ChainBuilder, PopulationBuilder, ScenarioBuilder}
import io.gatling.http.Predef._
import org.apache.polaris.benchmarks.actions._
import org.apache.polaris.benchmarks.parameters.BenchmarkConfig.config
import org.apache.polaris.benchmarks.parameters.{ConnectionParameters, DatasetParameters, Distribution, RandomNumberProvider, WorkloadParameters}
import org.apache.polaris.benchmarks.util.CircularIterator
import org.slf4j.LoggerFactory

import java.util.concurrent.atomic.{AtomicBoolean, AtomicReference}
import scala.concurrent.duration._

/**
 * This simulation performs reads and writes based on distributions specified in the config. It allows
 * the simulation of workloads where e.g. a small fraction of tables get most writes. It is intended to
 * be used against a Polaris instance with a pre-existing tree dataset.
 */
class WeightedWorkloadOnTreeDataset extends Simulation {
  private val logger = LoggerFactory.getLogger(getClass)

  // --------------------------------------------------------------------------------
  // Load parameters
  // --------------------------------------------------------------------------------
  val cp: ConnectionParameters = config.connectionParameters
  val dp: DatasetParameters = config.datasetParameters
  val wp: WorkloadParameters = config.workloadParameters

  // --------------------------------------------------------------------------------
  // Helper values
  // --------------------------------------------------------------------------------
  private val accessToken: AtomicReference[String] = new AtomicReference()
  private val shouldRefreshToken: AtomicBoolean = new AtomicBoolean(true)
  private val readersLaunched = new AtomicBoolean(false)

  private val authActions = AuthenticationActions(cp, accessToken)
  private val tblActions = TableActions(dp, wp, accessToken)

  // --------------------------------------------------------------------------------
  // Authentication related workloads:
  // * Authenticate and store the access token for later use every minute
  // * Wait for an OAuth token to be available
  // * Stop the token refresh loop
  // --------------------------------------------------------------------------------
  val continuouslyRefreshOauthToken: ScenarioBuilder =
    scenario("Authenticate every minute using the Iceberg REST API")
      .asLongAs(_ => shouldRefreshToken.get()) {
        feed(authActions.feeder())
          .exec(authActions.authenticateAndSaveAccessToken)
          .pause(1.minute)
      }

  val waitForAuthentication: ScenarioBuilder =
    scenario("Wait for the authentication token to be available")
      .asLongAs(_ => accessToken.get() == null) {
        pause(1.second)
      }

  val stopRefreshingToken: ScenarioBuilder =
    scenario("Stop refreshing the authentication token")
      .exec { session =>
        shouldRefreshToken.set(false)
        session
      }

  // --------------------------------------------------------------------------------
  // Build up the HTTP protocol configuration and set up the simulation
  // --------------------------------------------------------------------------------
  private val httpProtocol = http
    .baseUrl(cp.baseUrl)
    .acceptHeader("application/json")
    .contentTypeHeader("application/json")

  // --------------------------------------------------------------------------------
  // Create all reader scenarios and prepare them for injection
  // --------------------------------------------------------------------------------
  private val readerScenarioBuilders: List[ScenarioBuilder] = {
    wp.weightedWorkloadOnTreeDataset.readers.zipWithIndex.flatMap { case (dist, i) =>
      (0 until dist.count).map { threadId =>
        val rnp = RandomNumberProvider(wp.weightedWorkloadOnTreeDataset.seed, i * 1000 + threadId)
        scenario(s"Reader-$i-$threadId")
          .exec(authActions.restoreAccessTokenInSession)
          .during(wp.weightedWorkloadOnTreeDataset.durationInMinutes.minutes) {
            exec { session =>
              while (session.contains("accessToken")) {
                Thread.sleep(100)
              }

              val tableIndex = dist.sample(dp.totalTables, rnp)
              val (catalog, namespace, table) =
                Distribution.tableIndexToIdentifier(tableIndex, dp)
              session
                .set("catalogName", catalog)
                .set("multipartNamespace", namespace.mkString(0x1F.toChar.toString))
                .set("tableName", table)
            }
          }.exec(tblActions.fetchTable)
      }
    }
  }.toList

  // --------------------------------------------------------------------------------
  // Setup
  // --------------------------------------------------------------------------------
  setUp(
    List(
      continuouslyRefreshOauthToken.inject(atOnceUsers(1)).protocols(httpProtocol),
      waitForAuthentication
        .inject(atOnceUsers(1))
        .protocols(httpProtocol)
        .andThen(
          stopRefreshingToken
            .inject(atOnceUsers(1))
            .protocols(httpProtocol)
        )
    ) ++ readerScenarioBuilders.map(_.inject(atOnceUsers(1)).protocols(httpProtocol))
  )
}
