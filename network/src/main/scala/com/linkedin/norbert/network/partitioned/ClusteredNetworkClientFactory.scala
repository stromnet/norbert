/*
 * Copyright 2009-2014 LinkedIn, Inc
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
package partitioned

import client.NetworkClientConfig
import cluster.ClusterClient
import common.RetryStrategy
import loadbalancer.{DefaultClusteredLoadBalancerFactory, PartitionedLoadBalancerFactory}

/**
 * Defines a factory API that allows caller to create an instance of a <code>ClusteredNetworkClient<code>.
 *
 * @see ClusteredNetworkClient
 * @author SungJu (Joe) Cho
 */
class ClusteredNetworkClientFactory[PartitionedId](clientName: String,
                                                   serviceName: String,
                                                   zooKeeperConnectString: String,
                                                   zooKeeperSessionTimeoutMillis: Int,
                                                   closeChannelTimeMillis: Long,
                                                   norbertOutlierMultiplier: Double,
                                                   norbertOutlierConstant: Double,
                                                   clusteredLoadBalancerFactory: DefaultClusteredLoadBalancerFactory[PartitionedId],
                                                   enableSelectiveRetry: Boolean = false,
                                                   retryStrategy: RetryStrategy = null)
{
  def createPartitionedNetworkClient: ClusteredNetworkClient[PartitionedId] = {
    // Initializes the basic configs.
    val config = new NetworkClientConfig
    config.closeChannelTimeMillis = closeChannelTimeMillis
    config.outlierMuliplier = norbertOutlierMultiplier
    config.outlierConstant = norbertOutlierConstant
    config.clusterClient = ClusterClient(clientName, serviceName, zooKeeperConnectString, zooKeeperSessionTimeoutMillis)

    // Checks the validity of selective retry related configuration.
    if (enableSelectiveRetry) {
      if (retryStrategy == null)
        throw new IllegalArgumentException("Retry strategy needs to be provided if you enable selective retry")
      else
        config.retryStrategy = Some(retryStrategy)
    }

    val clusteredNetworkClient = ClusteredNetworkClient(config, clusteredLoadBalancerFactory)
    clusteredNetworkClient
  }
}

