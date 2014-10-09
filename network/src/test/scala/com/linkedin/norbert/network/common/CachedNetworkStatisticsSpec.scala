/*
 * Copyright 2009-2010 LinkedIn, Inc
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package com.linkedin.norbert
package network
package common

import org.specs.SpecificationWithJUnit
import org.specs.mock.Mockito
import client.NetworkClient
import client.loadbalancer.{LoadBalancerFactory, LoadBalancer, LoadBalancerFactoryComponent}
import server._
import cluster.{Node, ClusterClientComponent, ClusterClient}
import com.google.protobuf.Message
import scala.collection.mutable.MutableList
import com.linkedin.norbert.norbertutils.MockClock

class CachedNetworkStatisticsSpec extends SpecificationWithJUnit {
  val cachedNetworkStatistics = CachedNetworkStatistics[Int,Int](new MockClock, 100, 100)
  /*val clusterClient = mock[ClusterClient]

  val messageExecutor = new MessageExecutor {
    var called = false
    var request: Any = _
    val filters = new MutableList[Filter]
    def shutdown = {}

    def executeMessage[RequestMsg, ResponseMsg](request: RequestMsg, responseHandler: Option[(Either[Exception, ResponseMsg]) => Unit], context: Option[RequestContext])(implicit is: InputSerializer[RequestMsg, ResponseMsg]) = {
      called = true
      this.request = request

      val response = null.asInstanceOf[ResponseMsg]

      responseHandler.get(Right(response))
    }
  }

  val networkClient = new NetworkClient with ClusterClientComponent with ClusterIoClientComponent with LoadBalancerFactoryComponent
          with MessageExecutorComponent with LocalMessageExecution {
    val lb = mock[LoadBalancer]
    val loadBalancerFactory = mock[LoadBalancerFactory]
    val clusterIoClient = mock[ClusterIoClient]
    //    val messageRegistry = mock[MessageRegistry]
    val clusterClient = LocalMessageExecutionSpec.this.clusterClient
    val messageExecutor = LocalMessageExecutionSpec.this.messageExecutor
    val myNode = Node(1, "localhost:31313", true)
  }


  val nodes = Set(Node(1, "", true), Node(2, "", true), Node(3, "", true))
  val endpoints = nodes.map { n => new Endpoint {
    def node = n
    def canServeRequests = true
  }}
  val message = mock[Message]

  //  networkClient.messageRegistry.contains(any[Message]) returns true
  clusterClient.nodes returns nodes
  clusterClient.isConnected returns true
  networkClient.clusterIoClient.nodesChanged(nodes) returns endpoints
  networkClient.loadBalancerFactory.newLoadBalancer(endpoints) returns networkClient.lb
  */

  "CachedNetworkStatistics" should {
    "clear out maps properly" in {
      cachedNetworkStatistics.beginRequest(1,1,0)
      cachedNetworkStatistics.beginNetty(1,1,0)
      cachedNetworkStatistics.beginRequest(1,2,0)
      cachedNetworkStatistics.beginNetty(1,2,0)
      cachedNetworkStatistics.pendingTimings.get.get.get(1).get.length must be_==(2)
      cachedNetworkStatistics.endRequest(1,1)
      cachedNetworkStatistics.endRequest(1,2)
      cachedNetworkStatistics.pendingTimings.get.get.get(1).get.length must be_==(0)
    }
  }



}
