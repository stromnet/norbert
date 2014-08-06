/*
 * Copyright 2014- LinkedIn, Inc
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

package com.linkedin.norbert.network.netty

import org.jboss.netty.channel._
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.Request
import com.linkedin.norbert.network.common.ClusterIoClientComponent
import com.linkedin.norbert.cluster._
import java.util.concurrent.{ConcurrentHashMap => JConcurrentHashMap}
import java.util.UUID
import com.linkedin.norbert.protos.NorbertProtos
import com.linkedin.norbert.network.client.NetworkClientConfig
import scala.Tuple2

/**
 * Handle the forking of Dark Canary requests to the mirror hosts as well as collecting and (possibly) comparing
 * responses.
 */
class DarkCanaryChannelHandler extends Logging {
  private val requestMap = new JConcurrentHashMap[UUID, Tuple2[Request[Any,Any], Request[Any,Any]]]()
  private val mirroredHosts= new JConcurrentHashMap[Int, Node]()

  private var clusterIoClient: ClusterIoClientComponent#ClusterIoClient = null

  private var clusterClient : ClusterClient = null



  def initialize(clientConfig : NetworkClientConfig, clusterIoClient_ : ClusterIoClientComponent#ClusterIoClient) = {
    clusterIoClient = clusterIoClient_
    clientConfig.darkCanaryServiceName match {
      case None => {
        log.info("Dark canaries not configured for client %s".format(clientConfig.clientName))
      }
      case Some(serviceName) => {
        clusterClient = ClusterClient(clientConfig.clientName + "DarkCanary",
          serviceName,
          clientConfig.zooKeeperConnectString,
          clientConfig.zooKeeperSessionTimeoutMillis)

        clusterClient.addListener(new ClusterListener {
          /**
           * Handle a cluster event.
           *
           * @param event the <code>ClusterEvent</code> to handle
           */
          def handleClusterEvent(event: ClusterEvent) = event match {
            case ClusterEvents.Connected(nodes) => updateCurrentState(nodes)
            case ClusterEvents.NodesChanged(nodes) => updateCurrentState(nodes)
            case ClusterEvents.Disconnected => updateCurrentState(Set.empty[Node])
          }

          private def updateCurrentState(nodes : Set[Node]) : Unit = {
            this.synchronized {
              mirroredHosts.clear()
              nodes.foreach { node =>
                mirroredHosts.put(node.id, node)
              }
            }
          }
        })

        log.info("Dark canaries configured for client: %s. Dark Canary configurations come from Zookeeper service name : %s".format(
          clientConfig.clientName,
          serviceName
        ))
      }
    }
  }

  class DownStreamHandler extends SimpleChannelDownstreamHandler {

    override def writeRequested(ctx: ChannelHandlerContext, msg: MessageEvent) {
      if (!mirroredHosts.isEmpty) {
        msg.getMessage match {
          case request : Request[Any,Any] => {
            if (mirroredHosts.containsKey(request.node.id)) {
              val mirroredNode = mirroredHosts.get(request.node.id)
              if (!request.node.url.equals(mirroredNode.url)) {
                // This is a production request which we have to mirror.
                try {
                  log.info ("mirroring message from : %s to %s".format(request.node.url, mirroredNode.url))
                  val newRequest = Request(request.message,
                    mirroredNode,
                    request.inputSerializer,
                    request.outputSerializer,
                    None,
                    0)

                  requestMap.put(newRequest.id, (request, newRequest))
                  clusterIoClient.sendMessage(newRequest.node, newRequest)
                }
                catch {
                  case e : Exception => {
                    log.error("Exception while mirroring request to %s. Message: %s".format(mirroredNode.url,
                      e.getMessage))
                    log.error("Stack trace : %s".format(e.getStackTraceString))
                  }
                }
              }
            }
          }
        }
      }
      super.writeRequested(ctx, msg)
    }
  }

  class UpstreamHandler extends SimpleChannelUpstreamHandler {
    override def messageReceived(ctx: ChannelHandlerContext, msg: MessageEvent) {
      if (!requestMap.isEmpty) {
        msg.getMessage match {
          case message : NorbertProtos.NorbertMessage => {
            // Check if the request ID of the message corresponds to an existing mirrored request. If it does, then
            // just drop the message from the pipeline. This ensures that clients never see the message.

            val requestId = new UUID(message.getRequestIdMsb, message.getRequestIdLsb)
            requestMap.get(requestId) match {
              case request: Tuple2[Request[Any,Any], Request[Any,Any]] =>  {
                requestMap.remove(requestId)
                if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
                  log.info("Dropping successful response from %s".format(request._2.node.url))
                } else {
                  try {
                    // We did not get a successful response. Log an exception so that this is picked up by EKG.
                    throw new Exception("Got bad status %s for mirrored request %s".format(
                      message.getStatus.toString,
                      request._2.toString()))
                  }
                  catch {
                    case e : Exception => {
                      log.error("DarkCanaryException : %s".format(e.getMessage))
                      log.error ("Stack trace : %s".format(e.getStackTraceString))
                    }
                  }
                }
                // This is a mirrored request. Don't propagate the response.
              }
              case _ => {
                // This is not a mirrored request, Propagate the message upstream.
                super.messageReceived(ctx, msg)
              }
            }
          }
          // Propagate the message by default.
          case _ => super.messageReceived(ctx, msg)
        }
      } else {
        super.messageReceived(ctx, msg)
      }
    }
  }

  // These methods are currently only used to facilitate unit tests. There should be no calls to them from the rest of
  // code.
  def addNode(n : Node) : Unit = mirroredHosts.put(n.id, n)
  def removeNode(id: Int) : Unit = mirroredHosts.remove(id)
  def getInFlightRequestIds : Array[UUID] = requestMap.keySet().toArray.map { e => e.asInstanceOf[UUID]}
}
