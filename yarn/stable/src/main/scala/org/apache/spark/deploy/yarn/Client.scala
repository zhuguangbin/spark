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

package org.apache.spark.deploy.yarn

import java.nio.ByteBuffer

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.client.api.impl.YarnClientImpl
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Apps, Records}

import org.apache.spark.{Logging, SparkConf}


/**
 * The entry point (starting in Client#main() and Client#run()) for launching Spark on YARN. The
 * Client submits an application to the global ResourceManager to launch Spark's ApplicationMaster,
 * which will launch a Spark master process and negotiate resources throughout its duration.
 */
class Client(clientArgs: ClientArguments, hadoopConf: Configuration, spConf: SparkConf)
  extends YarnClientImpl with ClientBase with Logging {

  def this(clientArgs: ClientArguments, spConf: SparkConf) =
    this(clientArgs, new Configuration(), spConf)

  def this(clientArgs: ClientArguments) = this(clientArgs, new SparkConf())

  val args = clientArgs
  val conf = hadoopConf
  val sparkConf = spConf
  var rpc: YarnRPC = YarnRPC.create(conf)
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)

  def runApp(): ApplicationId = {
    validateArgs()
    // Initialize and start the client service.
    init(yarnConf)
    start()

    // Log details about this YARN cluster (e.g, the number of slave machines/NodeManagers).
    logClusterResourceDetails()

    // Prepare to submit a request to the ResourcManager (specifically its ApplicationsManager (ASM)
    // interface).

    // Get a new client application.
    val newApp = super.createApplication()
    val newAppResponse = newApp.getNewApplicationResponse()
    val appId = newAppResponse.getApplicationId()

    verifyClusterResources(newAppResponse)

    // Set up resource and environment variables.
    val appStagingDir = getAppStagingDir(appId)
    val localResources = prepareLocalResources(appStagingDir)
    val launchEnv = setupLaunchEnv(localResources, appStagingDir)
    val amContainer = createContainerLaunchContext(newAppResponse, localResources, launchEnv)

    // Set up an application submission context.
    val appContext = newApp.getApplicationSubmissionContext()
    appContext.setApplicationName(args.appName)
    appContext.setQueue(args.amQueue)
    appContext.setAMContainerSpec(amContainer)
    appContext.setApplicationType("SPARK")

    // Memory for the ApplicationMaster.
    val memoryResource = Records.newRecord(classOf[Resource]).asInstanceOf[Resource]
    memoryResource.setMemory(args.amMemory + YarnAllocationHandler.MEMORY_OVERHEAD)
    appContext.setResource(memoryResource)

    // Finally, submit and monitor the application.
    submitApp(appContext)
    appId
  }

  def run() {
    val appId = runApp()
    monitorApplication(appId)
    System.exit(0)
  }

  def logClusterResourceDetails() {
    val clusterMetrics: YarnClusterMetrics = super.getYarnClusterMetrics
    logInfo("Got Cluster metric info from ApplicationsManager (ASM), number of NodeManagers: " +
      clusterMetrics.getNumNodeManagers)

    val queueInfo: QueueInfo = super.getQueueInfo(args.amQueue)
    logInfo( """Queue info ... queueName: %s, queueCurrentCapacity: %s, queueMaxCapacity: %s,
      queueApplicationCount = %s, queueChildQueueCount = %s""".format(
        queueInfo.getQueueName,
        queueInfo.getCurrentCapacity,
        queueInfo.getMaximumCapacity,
        queueInfo.getApplications.size,
        queueInfo.getChildQueues.size))
  }

  def calculateAMMemory(newApp: GetNewApplicationResponse) :Int = {
    // TODO: Need a replacement for the following code to fix -Xmx?
    // val minResMemory: Int = newApp.getMinimumResourceCapability().getMemory()
    // var amMemory = ((args.amMemory / minResMemory) * minResMemory) +
    //  ((if ((args.amMemory % minResMemory) == 0) 0 else minResMemory) -
    //    YarnAllocationHandler.MEMORY_OVERHEAD)
    args.amMemory
  }

  def setupSecurityToken(amContainer: ContainerLaunchContext) = {
    // Setup security tokens.
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    amContainer.setTokens(ByteBuffer.wrap(dob.getData()))
  }

  def submitApp(appContext: ApplicationSubmissionContext) = {
    // Submit the application to the applications manager.
    logInfo("Submitting application to ASM")
    super.submitApplication(appContext)
  }

  def monitorApplication(appId: ApplicationId): Boolean = {
    val interval = sparkConf.getLong("spark.yarn.report.interval", 1000)

    while (true) {
      Thread.sleep(interval)
      val report = super.getApplicationReport(appId)

      logInfo("Application report from ASM: \n" +
        "\t application identifier: " + appId.toString() + "\n" +
        "\t appId: " + appId.getId() + "\n" +
        "\t clientToAMToken: " + report.getClientToAMToken() + "\n" +
        "\t appDiagnostics: " + report.getDiagnostics() + "\n" +
        "\t appMasterHost: " + report.getHost() + "\n" +
        "\t appQueue: " + report.getQueue() + "\n" +
        "\t appMasterRpcPort: " + report.getRpcPort() + "\n" +
        "\t appStartTime: " + report.getStartTime() + "\n" +
        "\t yarnAppState: " + report.getYarnApplicationState() + "\n" +
        "\t distributedFinalState: " + report.getFinalApplicationStatus() + "\n" +
        "\t appTrackingUrl: " + report.getTrackingUrl() + "\n" +
        "\t appUser: " + report.getUser()
      )

      val state = report.getYarnApplicationState()
      if (state == YarnApplicationState.FINISHED ||
        state == YarnApplicationState.FAILED ||
        state == YarnApplicationState.KILLED) {
        return true
      }
    }
    true
  }
}

object Client {

  def main(argStrings: Array[String]) {
    // Set an env variable indicating we are running in YARN mode.
    // Note: anything env variable with SPARK_ prefix gets propagated to all (remote) processes -
    // see Client#setupLaunchEnv().
    System.setProperty("SPARK_YARN_MODE", "true")
    val sparkConf = new SparkConf()
    val args = new ClientArguments(argStrings, sparkConf)

    new Client(args, sparkConf).run()
  }

}
