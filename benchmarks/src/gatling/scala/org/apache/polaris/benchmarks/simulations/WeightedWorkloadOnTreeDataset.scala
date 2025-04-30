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
  // Helper method to restore the token to the session
  // --------------------------------------------------------------------------------
  private def restoreAccessTokenInSession = exec { session =>
    if (accessToken.get() == null) {
      logger.warn("Access token is null when trying to restore to session!")
    }
    session.set("accessToken", accessToken.get())
  }

  // --------------------------------------------------------------------------------
  // Create all reader scenarios and prepare them for injection
  // --------------------------------------------------------------------------------
  private val readerScenarioBuilders: List[ScenarioBuilder] = {
    wp.weightedWorkloadOnTreeDataset.readers.zipWithIndex.flatMap { case (dist, i) =>
      (0 until dist.count).map { threadId =>
        val rnp = RandomNumberProvider(wp.weightedWorkloadOnTreeDataset.seed, i * 1000 + threadId)

        scenario(s"Reader-$i-$threadId")
          .exec(restoreAccessTokenInSession)
          .during(wp.weightedWorkloadOnTreeDataset.durationInMinutes.minutes) {
            exec { session =>
              val tableIndex = dist.sample(dp.totalTables, rnp)
              val (catalog, namespace, table) =
                Distribution.tableIndexToIdentifier(tableIndex, dp)
              session
                .set("catalogName", catalog)
                .set("multipartNamespace", namespace.mkString(0x1F.toChar.toString))
                .set("tableName", table)
            }.exec(tblActions.fetchTable)
          }
      }
    }.toList
  }

  // Convert the ScenarioBuilders to PopulationBuilders ready for injection
  private val readerPopulations: List[PopulationBuilder] =
    readerScenarioBuilders.map(_.inject(atOnceUsers(1)).protocols(httpProtocol))

  // --------------------------------------------------------------------------------
  // Create a launcher scenario that will start all readers after authentication
  // This is the key part that prevents multiple setUp calls
  // --------------------------------------------------------------------------------
  // Define a scenario that triggers all readers to start
  val launchReadersScenario: ScenarioBuilder = scenario("Launch readers")
    .exec { session =>
      // Access token is available here
      logger.info(s"Authentication complete with token: ${accessToken.get() != null}")

      // Ensure we only launch the readers once
      if (!readersLaunched.getAndSet(true)) {
        logger.info(s"Starting ${readerScenarioBuilders.size} reader threads")

        // Each reader will get the access token when it runs
        // We don't create any more setUp calls here
      }
      session
    }

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
          launchReadersScenario
            .inject(atOnceUsers(1))
            .protocols(httpProtocol)
        )
        .andThen(
          stopRefreshingToken
            .inject(atOnceUsers(1))
            .protocols(httpProtocol)
        )
    ) ++ readerPopulations
  )
}
