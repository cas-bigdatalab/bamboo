package cn.pandadb.costore.cluster

import cn.pandadb.costore.node.NodeRPC
import net.neoremind.kraps.RpcConf
import net.neoremind.kraps.rpc.netty.NettyRpcEnvFactory
import net.neoremind.kraps.rpc.{RpcAddress, RpcCallContext, RpcEndpoint, RpcEndpointRef, RpcEnv, RpcEnvClientConfig}

//class ClusterRPC extends NodeRPC (ip: String, port: Int) { //TODO: hippo rpc
//
//  private val nodeRPCs = this.nodes.map{ node => {
//    val ipPort = node.split(':')
//    new NodeRPC(ipPort.head, ipPort.tail.head.toInt)
//  }}
//
//  val leaderNode = nodeRPCs.head
//}