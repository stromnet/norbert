package com.linkedin.norbert.network.netty

import org.jboss.netty.channel._
import com.linkedin.norbert.logging.Logging
import com.linkedin.norbert.network.Request
import com.linkedin.norbert.network.common.ClusterIoClient
import com.linkedin.norbert.cluster.{ClusterEvents, ClusterEvent, ClusterListener, Node}
import java.util.concurrent.{ConcurrentHashMap => JConcurrentHashMap}
import java.util.UUID
import com.linkedin.norbert.protos.NorbertProtos
import scala.Tuple2
import com.linkedin.norbert.network.client.NetworkClientConfig
import com.linkedin.norbert.cluster.zookeeper.ZooKeeperClusterClient

/**
 * Handle the forking of Dark Canary requests to the mirror hosts as well as collecting and (possibly) comparing
 * responses.
 */
object DarkCanaryChannelHandler {
  private val requestMap = new JConcurrentHashMap[UUID, Tuple2[Request[Any,Any], Request[Any,Any]]]()
  private val mirroredHosts= new JConcurrentHashMap[Int, Node]()

  private var initialized : Boolean = false
  private var clusterIoClient: ClusterIoClient = null

  private var clusterClient : ZooKeeperClusterClient = null

  def initialize(clientConfig : NetworkClientConfig, clusterIoClient_ : ClusterIoClient) = {
    clusterClient = new ZooKeeperClusterClient(Some(clientConfig.clientName + "DarkCanary"),
      clientConfig.darkCanaryServiceName,
      clientConfig.zooKeeperConnectString,
      clientConfig.zooKeeperSessionTimeoutMillis
    )

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
        println("Updating node state to : %s".format(nodes.toString()))
        mirroredHosts.clear()
        nodes.foreach { node => mirroredHosts.put(node.id, node)
        }
      }
    })
    clusterIoClient = clusterIoClient_
    initialized = true
  }


  class DownStreamHandler ()  extends SimpleChannelDownstreamHandler with Logging with UrlParser {

     override def writeRequested(ctx: ChannelHandlerContext, msg: MessageEvent) {
       if (!initialized) {
         super.writeRequested(ctx, msg)
         return
       }

       msg.getMessage match {
        case request : Request[Any,Any] => {
          // 1) Check if request.node.url corresponds to a host that is being mirrored.
          //    Simply propagate message if it isn't.
          // 2) If the request is to be mirrored, make a copy of it with information about the mirrored node and
          //    pass the new request on the clusterIoClient after saving the new request id.

          if (mirroredHosts.contains(request.node.id)) {
            try {
              val newRequest = Request(request.message,
                mirroredHosts.get(request.node.id),
                request.inputSerializer,
                request.outputSerializer,
                None,
                0)

              requestMap.put(newRequest.id, (request, newRequest))
              clusterIoClient.sendMessage(newRequest.node, newRequest)
            }
            catch {
              case e : Exception => {
                log.error("Exception while mirroring request to %s. Message: %s".format(mirroredHosts.get(request.node.id).url, e.getMessage))
                log.error("Stack trace : %s".format(e.getStackTraceString))
              }
            }
          }
        }
      }
      super.writeRequested(ctx, msg)
    }
  }

  class UpstreamHandler extends SimpleChannelUpstreamHandler with Logging {
    override def messageReceived(ctx: ChannelHandlerContext, msg: MessageEvent) {
      if (!initialized) {
        super.messageReceived(ctx, msg)
        return
      }
      msg.getMessage match {
        case message : NorbertProtos.NorbertMessage => {
          // Check if the request ID of the message corresponds to an existing mirrored request. If it does, then
          // just drop the message from the pipeline. This ensures that clients never see the message.

          val requestId = new UUID(message.getRequestIdMsb, message.getRequestIdLsb)
          requestMap.get(requestId) match {
            case request : Tuple2[Request[Any,Any], Request[Any,Any]] =>  {
              if (message.getStatus == NorbertProtos.NorbertMessage.Status.OK) {
                log.info("Dropping successful response from %s".format(request._2.node.url))
              } else {
                log.info("Did not get successful response from %s".format(request._2.node.url))
              }
              // This is a mirrored request. Don't propagate the response.
            }
            case _ => {
              // This is not a mirrored request, Propagate the message upstream.
              super.messageReceived(ctx, msg)
            }
          }
        }
        case _ => super.messageReceived(ctx, msg)
      }
    }
  }
}
