/**
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

package kafka.network

import java.io.IOException
import java.net._
import java.nio.channels._
import java.nio.channels.{Selector => NSelector}
import java.util
import java.util.concurrent._
import java.util.concurrent.atomic._

import com.yammer.metrics.core.Gauge
import kafka.cluster.{BrokerEndPoint, EndPoint}
import kafka.common.KafkaException
import kafka.metrics.KafkaMetricsGroup
import kafka.server.KafkaConfig
import kafka.utils._
import org.apache.kafka.common.metrics._
import org.apache.kafka.common.network.{ChannelBuilders, KafkaChannel, LoginType, Mode, Selector => KSelector}
import org.apache.kafka.common.security.auth.KafkaPrincipal
import org.apache.kafka.common.protocol.SecurityProtocol
import org.apache.kafka.common.protocol.types.SchemaException
import org.apache.kafka.common.utils.{Time, Utils}

import scala.collection._
import JavaConverters._
import scala.util.control.{ControlThrowable, NonFatal}

/**
 * An NIO socket server. The threading model is
 *   1 Acceptor thread that handles new connections
 *   Acceptor has N Processor threads that each have their own selector and read requests from sockets
 *   M Handler threads that handle requests and produce responses back to the processor threads for writing.
 */
class SocketServer(val config: KafkaConfig, val metrics: Metrics, val time: Time) extends Logging with KafkaMetricsGroup {

  private val endpoints = config.listeners
  private val numProcessorThreads = config.numNetworkThreads
  private val maxQueuedRequests = config.queuedMaxRequests
  private val totalProcessorThreads = numProcessorThreads * endpoints.size

  private val maxConnectionsPerIp = config.maxConnectionsPerIp
  private val maxConnectionsPerIpOverrides = config.maxConnectionsPerIpOverrides

  this.logIdent = "[Socket Server on Broker " + config.brokerId + "], "
  //创建Processor的数组，其中有totalProcessorThreads个responseQueue队列
  val requestChannel = new RequestChannel(totalProcessorThreads, maxQueuedRequests)
  //创建保存processors数组    长度为totalProcessorThreads
  private val processors = new Array[Processor](totalProcessorThreads)
  //创建保存acceptors集合
  private[network] val acceptors = mutable.Map[EndPoint, Acceptor]()
  private var connectionQuotas: ConnectionQuotas = _

  private val allMetricNames = (0 until totalProcessorThreads).map { i =>
    val tags = new util.HashMap[String, String]()
    tags.put("networkProcessor", i.toString)
    metrics.metricName("io-wait-ratio", "socket-server-metrics", tags)
  }

  /**
   * Start the socket server startup()是socket server核心代码
   */
  def startup() {
    this.synchronized {//同步
      //创建connectionQuotas
      connectionQuotas = new ConnectionQuotas(maxConnectionsPerIp, maxConnectionsPerIpOverrides)
      //socket的sendBuffer大小
      val sendBufferSize = config.socketSendBufferBytes
      //socket的receiveBuffer大小
      val recvBufferSize = config.socketReceiveBufferBytes
      val brokerId = config.brokerId

      var processorBeginIndex = 0
      //遍历endpoints集合
      endpoints.values.foreach { endpoint =>
        val protocol = endpoint.protocolType
        val processorEndIndex = processorBeginIndex + numProcessorThreads
        //processors数组 从 processorBeginIndex~processorEndIndex,都是当前endpoint对应的processor对象集合
        for (i <- processorBeginIndex until processorEndIndex)
          //创建processor对象
          processors(i) = newProcessor(i, connectionQuotas, protocol)
         //创建acceptor，同时为Processor创建对应的线程。第五个参数指定了Processors数组中与此Acceptor对象对应的Processor对象
        val acceptor = new Acceptor(endpoint, sendBufferSize, recvBufferSize, brokerId,
          processors.slice(processorBeginIndex, processorEndIndex), connectionQuotas)
        acceptors.put(endpoint, acceptor)
        //创建Acceptor对应的线程，并启动
        Utils.newThread("kafka-socket-acceptor-%s-%d".format(protocol.toString, endpoint.port), acceptor, false).start()
        acceptor.awaitStartup()// 主线程阻塞等待Acceptor线程启动完成。
        //修改processorBeginIndex,为了下一个Endpoint准备
        processorBeginIndex = processorEndIndex
      }
    }

    newGauge("NetworkProcessorAvgIdlePercent",
      new Gauge[Double] {
        def value = allMetricNames.map( metricName =>
          metrics.metrics().get(metricName).value()).sum / totalProcessorThreads
      }
    )

    info("Started " + acceptors.size + " acceptor threads")
  }

  // register the processor threads for notification of responses
  // 向requestChannel添加一个监听器。这个监听器的功能是：当Handler线程向某个responseQueue写数据时，会唤醒对应的Processor线程进行处理
  requestChannel.addResponseListener(id => processors(id).wakeup())

  /**
   * Shutdown the socket server
   */
  def shutdown() = {
    info("Shutting down")
    this.synchronized {//同步
      acceptors.values.foreach(_.shutdown)//调用所有acceptor的shutdown
      processors.foreach(_.shutdown)//调用所有processor的shutdown
    }
    info("Shutdown completed")
  }

  def boundPort(protocol: SecurityProtocol = SecurityProtocol.PLAINTEXT): Int = {
    try {
      acceptors(endpoints(protocol)).serverChannel.socket().getLocalPort
    } catch {
      case e: Exception => throw new KafkaException("Tried to check server's port before server was started or checked for port of non-existing protocol", e)
    }
  }

  /* `protected` for test usage */
  protected[network] def newProcessor(id: Int, connectionQuotas: ConnectionQuotas, protocol: SecurityProtocol): Processor = {
    new Processor(id,
      time,
      config.socketRequestMaxBytes,
      requestChannel,
      connectionQuotas,
      config.connectionsMaxIdleMs,
      protocol,
      config.values,
      metrics
    )
  }

  /* For test usage */
  private[network] def connectionCount(address: InetAddress): Int =
    Option(connectionQuotas).fold(0)(_.get(address))

  /* For test usage */
  private[network] def processor(index: Int): Processor = processors(index)

}

/**
 * A base class with some helper variables and methods
 */
private[kafka] abstract class AbstractServerThread(connectionQuotas: ConnectionQuotas) extends Runnable with Logging {

  private val startupLatch = new CountDownLatch(1)
  private val shutdownLatch = new CountDownLatch(1)
  private val alive = new AtomicBoolean(true)
   //抽象方法
  def wakeup()

  /**
   * Initiates a graceful shutdown by signaling to stop and waiting for the shutdown to complete
   */
  def shutdown(): Unit = {
    alive.set(false)//修改运行状态
    wakeup()//唤醒当前的AbstractServerThread
    shutdownLatch.await()//阻塞等待关闭完成
  }

  /**
   * Wait for the thread to completely start up
    * 阻塞等待启动操作完成
   */
  def awaitStartup(): Unit = startupLatch.await

  /**
    * 标明启动操作完成，同时唤醒被阻塞的线程
   * Record that the thread startup is complete
   */
  protected def startupComplete() = {
    startupLatch.countDown()
  }

  /**
   * Record that the thread shutdown is complete
    * 阻塞等待关闭操作完成
   */
  protected def shutdownComplete() = shutdownLatch.countDown()

  /**
   * Is the server still running?
   */
  protected def isRunning = alive.get

  /**
    * 关闭指定连接
   * Close the connection identified by `connectionId` and decrement the connection count.
   */
  def close(selector: KSelector, connectionId: String) {
    val channel = selector.channel(connectionId)
    if (channel != null) {
      debug(s"Closing selector connection $connectionId")
      val address = channel.socketAddress
      if (address != null)
        connectionQuotas.dec(address)//修改connectionQuotas记录的连接数
      selector.close(connectionId)//关闭连接
    }
  }

  /**
   * Close `channel` and decrement the connection count.
   */
  def close(channel: SocketChannel) {
    if (channel != null) {
      debug("Closing connection from " + channel.socket.getRemoteSocketAddress())
      connectionQuotas.dec(channel.socket.getInetAddress)
      swallowError(channel.socket().close())
      swallowError(channel.close())
    }
  }
}

/**
 * Thread that accepts and configures new connections. There is one of these per endpoint.
 */
private[kafka] class Acceptor(val endPoint: EndPoint,
                              val sendBufferSize: Int,
                              val recvBufferSize: Int,
                              brokerId: Int,
                              processors: Array[Processor],
                              connectionQuotas: ConnectionQuotas) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {
  //创建nioSelector
  private val nioSelector = NSelector.open()
  //创建ServerSocketChannel
  val serverChannel = openServerSocket(endPoint.host, endPoint.port)

  this.synchronized {//同步
    //为其对应的每个Processor都创建对应的线程并启动
    processors.foreach { processor =>
      Utils.newThread("kafka-network-thread-%d-%s-%d".format(brokerId, endPoint.protocolType.toString, processor.id), processor, false).start()
    }
  }

  /**
   * Accept loop that checks for new connection attempts
   */
  def run() {
    //注册OP_ACCEPT事件
    serverChannel.register(nioSelector, SelectionKey.OP_ACCEPT)
    startupComplete()//识别当前线程启动线程已经完成
    try {
      var currentProcessor = 0
      while (isRunning) {//检测线程运行状态
        try {
          val ready = nioSelector.select(500)//等待关注的事件
          if (ready > 0) {
            val keys = nioSelector.selectedKeys()
            val iter = keys.iterator()
            while (iter.hasNext && isRunning) {
              try {
                val key = iter.next
                iter.remove()
                if (key.isAcceptable)//调用accept()方法处理OP_ACCEPT事件
                  accept(key, processors(currentProcessor))
                else//如果不是OP_ACCEPT事件，就报错
                  throw new IllegalStateException("Unrecognized key state for acceptor thread.")

                // round robin to the next processor thread
                //更新currentProcessor，这里使用了Round-Robin的方法选择Processor
                currentProcessor = (currentProcessor + 1) % processors.length
              } catch {
                case e: Throwable => error("Error while accepting connection", e)
              }
            }
          }
        }
        catch {
          // We catch all the throwables to prevent the acceptor thread from exiting on exceptions due
          // to a select operation on a specific channel or a bad request. We don't want the
          // the broker to stop responding to requests from other clients in these scenarios.
          case e: ControlThrowable => throw e
          case e: Throwable => error("Error occurred", e)
        }
      }
    } finally {
      debug("Closing server socket and selector.")
      swallowError(serverChannel.close())
      swallowError(nioSelector.close())
      shutdownComplete()//线程关闭已经完成
    }
  }

  /*
   * Create a server socket to listen for connections on.
   */
  private def openServerSocket(host: String, port: Int): ServerSocketChannel = {
    val socketAddress =
      if(host == null || host.trim.isEmpty)
        new InetSocketAddress(port)
      else
        new InetSocketAddress(host, port)
    val serverChannel = ServerSocketChannel.open()
    serverChannel.configureBlocking(false)
    serverChannel.socket().setReceiveBufferSize(recvBufferSize)
    try {
      serverChannel.socket.bind(socketAddress)
      info("Awaiting socket connections on %s:%d.".format(socketAddress.getHostString, serverChannel.socket.getLocalPort))
    } catch {
      case e: SocketException =>
        throw new KafkaException("Socket server failed to bind to %s:%d: %s.".format(socketAddress.getHostString, port, e.getMessage), e)
    }
    serverChannel
  }

  /*
   * Accept a new connection
   */
  def accept(key: SelectionKey, processor: Processor) {
    val serverSocketChannel = key.channel().asInstanceOf[ServerSocketChannel]
    val socketChannel = serverSocketChannel.accept()//创建SocketChannel
    try {
      //增加connectionQuotas中记录的连接数
      connectionQuotas.inc(socketChannel.socket().getInetAddress)
      socketChannel.configureBlocking(false)
      socketChannel.socket().setTcpNoDelay(true)
      socketChannel.socket().setKeepAlive(true)
      socketChannel.socket().setSendBufferSize(sendBufferSize)

      debug("Accepted connection from %s on %s and assigned it to processor %d, sendBufferSize [actual|requested]: [%d|%d] recvBufferSize [actual|requested]: [%d|%d]"
            .format(socketChannel.socket.getRemoteSocketAddress, socketChannel.socket.getLocalSocketAddress, processor.id,
                  socketChannel.socket.getSendBufferSize, sendBufferSize,
                  socketChannel.socket.getReceiveBufferSize, recvBufferSize))
      //将SocketChannel交给processor处理
      processor.accept(socketChannel)
    } catch {
      case e: TooManyConnectionsException =>
        info("Rejected connection from %s, address already has the configured maximum of %d connections.".format(e.ip, e.count))
        close(socketChannel)//关闭socketChannel
    }
  }

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = nioSelector.wakeup()

}

/**
 * Thread that processes all requests from a single connection. There are N of these running in parallel
 * each of which has its own selector
 */
private[kafka] class Processor(val id: Int,
                               time: Time,
                               maxRequestSize: Int,
                               requestChannel: RequestChannel,
                               connectionQuotas: ConnectionQuotas,
                               connectionsMaxIdleMs: Long,
                               protocol: SecurityProtocol,
                               channelConfigs: java.util.Map[String, _],
                               metrics: Metrics) extends AbstractServerThread(connectionQuotas) with KafkaMetricsGroup {

  private object ConnectionId {
    def fromString(s: String): Option[ConnectionId] = s.split("-") match {
      case Array(local, remote) => BrokerEndPoint.parseHostPort(local).flatMap { case (localHost, localPort) =>
        BrokerEndPoint.parseHostPort(remote).map { case (remoteHost, remotePort) =>
          ConnectionId(localHost, localPort, remoteHost, remotePort)
        }
      }
      case _ => None
    }
  }

  private case class ConnectionId(localHost: String, localPort: Int, remoteHost: String, remotePort: Int) {
    override def toString: String = s"$localHost:$localPort-$remoteHost:$remotePort"
  }

  private val newConnections = new ConcurrentLinkedQueue[SocketChannel]()
  private val inflightResponses = mutable.Map[String, RequestChannel.Response]()
  private val metricTags = Map("networkProcessor" -> id.toString).asJava

  newGauge("IdlePercent",
    new Gauge[Double] {
      def value = {
        metrics.metrics().get(metrics.metricName("io-wait-ratio", "socket-server-metrics", metricTags)).value()
      }
    },
    metricTags.asScala
  )

  private val selector = new KSelector(
    maxRequestSize,
    connectionsMaxIdleMs,
    metrics,
    time,
    "socket-server",
    metricTags,
    false,
    ChannelBuilders.create(protocol, Mode.SERVER, LoginType.SERVER, channelConfigs, null, true))

  override def run() {
    startupComplete()//1.标识Processor的初始化流程已经结束，唤醒阻塞等待此Processor初始化完成的线程。
    while (isRunning) {//检验alive字段标识的运行状态
      try {
        // setup any new connections that have been queued up
        // 2.处理newConnection队列中的新建SocketChannel。队列中的每个SocketChannel都会在nioSelector上注册OP_READ事件。
        configureNewConnections()
        // register any new responses for writing
        // 3.获取RequestChannel中对应的responseQueue队列，并处理其中缓存的response。

        processNewResponses()
        //4.SocketServer.poll()方法读取请求，发送响应。poll()方法底层调用KSelector.poll()方法。
        poll()
        //5.处理KSelector.completedReceives队列。
        processCompletedReceives()
        //6.处理KSelector.completedSends队列。
        processCompletedSends()
        //7.处理KSelector.disconnected队列。
        processDisconnected()
      } catch {
        // We catch all the throwables here to prevent the processor thread from exiting. We do this because
        // letting a processor exit might cause a bigger impact on the broker. Usually the exceptions thrown would
        // be either associated with a specific socket channel or a bad request. We just ignore the bad socket channel
        // or request. This behavior might need to be reviewed if we see an exception that need the entire broker to stop.
        case e: ControlThrowable => throw e
        case e: Throwable =>
          error("Processor got uncaught exception.", e)
      }
    }

    debug("Closing selector - processor " + id)
    swallowError(closeAll())
    shutdownComplete()//8.一系列关闭操作
  }

  private def processNewResponses() {
    /*
     在RequestChannel中使用Processor的Id绑定与responseQueue的对应关系
     获取对应的responseQueue中的响应
     */
    var curr = requestChannel.receiveResponse(id)
    while (curr != null) {
      try {
        curr.responseAction match {
          //没有响应需要发送给客户端
          case RequestChannel.NoOpAction =>
            // There is no response to send to the client, we need to read more pipelined requests
            // that are sitting in the server's socket buffer
            curr.request.updateRequestMetrics
            trace("Socket server received empty response to send, registering for read: " + curr)
            //注册OP_READ事件
            selector.unmute(curr.request.connectionId)
            //该响应需要发送给客户端
          case RequestChannel.SendAction =>
            //调用KSelector.send()方法，并将响应放入inflightResponse队列缓存
            sendResponse(curr)
          case RequestChannel.CloseConnectionAction =>
            curr.request.updateRequestMetrics
            trace("Closing socket connection actively according to the response code.")
            close(selector, curr.request.connectionId)
        }
      } finally {
        curr = requestChannel.receiveResponse(id)//继续处理responseQueue
      }
    }
  }

  /* `protected` for test usage */
  protected[network] def sendResponse(response: RequestChannel.Response) {
    trace(s"Socket server received response to send, registering for write and sending data: $response")
    val channel = selector.channel(response.responseSend.destination)
    // `channel` can be null if the selector closed the connection because it was idle for too long
    if (channel == null) {
      warn(s"Attempting to send response via channel for which there is no open connection, connection id $id")
      response.request.updateRequestMetrics()
    }
    else {
      selector.send(response.responseSend)
      inflightResponses += (response.request.connectionId -> response)
    }
  }

  private def poll() {
    try selector.poll(300)
    catch {
      case e @ (_: IllegalStateException | _: IOException) =>
        error(s"Closing processor $id due to illegal state or IO exception")
        swallow(closeAll())
        shutdownComplete()
        throw e
    }
  }

  private def processCompletedReceives() {
    //遍历completedReceives队列
    selector.completedReceives.asScala.foreach { receive =>
      try {
        //获取对应的 KafkaChannel
        val channel = selector.channel(receive.source)
        //创建KafkaChannel对应的session对象，与权限控制相关
        val session = RequestChannel.Session(new KafkaPrincipal(KafkaPrincipal.USER_TYPE, channel.principal.getName),
          channel.socketAddress)
        //将NetworkReceive,ProcessId,身份认证信息封装成RequestChannel.Request对象
        val req = RequestChannel.Request(processor = id, connectionId = receive.source, session = session, buffer = receive.payload, startTimeMs = time.milliseconds, securityProtocol = protocol)
        //将RequestChannel.Request放入RequestChannel.requestQueue队列中等待处理
        requestChannel.sendRequest(req)
        //取消注册的OP_READ事件，连接不再读取数据
        selector.mute(receive.source)
      } catch {
        case e @ (_: InvalidRequestException | _: SchemaException) =>
          // note that even though we got an exception, we can assume that receive.source is valid. Issues with constructing a valid receive object were handled earlier
          error(s"Closing socket for ${receive.source} because of error", e)
          close(selector, receive.source)
      }
    }
  }

  private def processCompletedSends() {
    //遍历completedSends队列
    selector.completedSends.asScala.foreach { send =>
      //这个响应已经发送出去了，从inflightResponses删除
      val resp = inflightResponses.remove(send.destination).getOrElse {
        throw new IllegalStateException(s"Send for ${send.destination} completed, but not in `inflightResponses`")
      }
      resp.request.updateRequestMetrics()
      selector.unmute(send.destination)//为对应的连接重新注册OP_READ事件，允许从该连接读取数据。
    }
  }

  private def processDisconnected() {
    //遍历selector.disconnected队列
    selector.disconnected.asScala.foreach { connectionId =>
      val remoteHost = ConnectionId.fromString(connectionId).getOrElse {
        throw new IllegalStateException(s"connectionId has unexpected format: $connectionId")
      }.remoteHost
      //从InflightResponses中删除该连接对应的Response
      inflightResponses.remove(connectionId).foreach(_.request.updateRequestMetrics())
      // the channel has been closed by the selector but the quotas still need to be updated
      //减少ConnectionQuotas中对应记录的连接数
      connectionQuotas.dec(InetAddress.getByName(remoteHost))
    }
  }

  /**
   * Queue up a new connection for reading
   */
  def accept(socketChannel: SocketChannel) {
    //将SocketChannel放入newConnections队列中
    newConnections.add(socketChannel)
    //通过调用wakepu()方法实现，最终调用java nio Selector的wakeup()方法
    wakeup()
  }

  /**
   * Register any new connections that have been queued up
   */
  private def configureNewConnections() {
    while (!newConnections.isEmpty) {//遍历newConnections队列
      val channel = newConnections.poll()
      try {
        debug(s"Processor $id listening to new connection from ${channel.socket.getRemoteSocketAddress}")
        val localHost = channel.socket().getLocalAddress.getHostAddress
        val localPort = channel.socket().getLocalPort
        val remoteHost = channel.socket().getInetAddress.getHostAddress
        val remotePort = channel.socket().getPort
        //根据localHost, localPort, remoteHost, remotePort的获取创建connectionId
        val connectionId = ConnectionId(localHost, localPort, remoteHost, remotePort).toString
        selector.register(connectionId, channel)//注册OP_READ事件
      } catch {
        // We explicitly catch all non fatal exceptions and close the socket to avoid a socket leak. The other
        // throwables will be caught in processor and logged as uncaught exceptions.
        case NonFatal(e) =>
          // need to close the channel here to avoid a socket leak.
          close(channel)
          error(s"Processor $id closed connection from ${channel.getRemoteAddress}", e)
      }
    }
  }

  /**
   * Close the selector and all open connections
   */
  private def closeAll() {
    selector.channels.asScala.foreach { channel =>
      close(selector, channel.id)
    }
    selector.close()
  }

  /* For test usage */
  private[network] def channel(connectionId: String): Option[KafkaChannel] =
    Option(selector.channel(connectionId))

  /**
   * Wakeup the thread for selection.
   */
  @Override
  def wakeup = selector.wakeup()

}

class ConnectionQuotas(val defaultMax: Int, overrideQuotas: Map[String, Int]) {

  private val overrides = overrideQuotas.map { case (host, count) => (InetAddress.getByName(host), count) }
  private val counts = mutable.Map[InetAddress, Int]()

  def inc(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElseUpdate(address, 0)
      counts.put(address, count + 1)
      val max = overrides.getOrElse(address, defaultMax)
      if (count >= max)
        throw new TooManyConnectionsException(address, max)
    }
  }

  def dec(address: InetAddress) {
    counts.synchronized {
      val count = counts.getOrElse(address,
        throw new IllegalArgumentException(s"Attempted to decrease connection count for address with no connections, address: $address"))
      if (count == 1)
        counts.remove(address)
      else
        counts.put(address, count - 1)
    }
  }

  def get(address: InetAddress): Int = counts.synchronized {
    counts.getOrElse(address, 0)
  }

}

class TooManyConnectionsException(val ip: InetAddress, val count: Int) extends KafkaException("Too many connections from %s (maximum = %d)".format(ip, count))
