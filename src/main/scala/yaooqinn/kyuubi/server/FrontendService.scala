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

package yaooqinn.kyuubi.server

import java.net.{InetAddress, ServerSocket, UnknownHostException}
import java.util.concurrent.{SynchronousQueue, ThreadPoolExecutor, TimeUnit}

import scala.collection.JavaConverters._
import scala.util.{Failure, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hive.service.cli.thrift._
import org.apache.spark.{KyuubiConf, SparkConf}
import org.apache.spark.KyuubiConf._
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.thrift.protocol.{TBinaryProtocol, TProtocol}
import org.apache.thrift.server.{ServerContext, TServer, TServerEventHandler, TThreadPoolServer}
import org.apache.thrift.transport.{TServerSocket, TTransport, TTransportException}

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.auth.{AuthType, KyuubiAuthFactory, TSetIpAddressProcessor}
import yaooqinn.kyuubi.cli.{FetchOrientation, FetchType, GetInfoType}
import yaooqinn.kyuubi.operation.OperationHandle
import yaooqinn.kyuubi.schema.SchemaMapper
import yaooqinn.kyuubi.service.{AbstractService, ServiceException, ServiceUtils}
import yaooqinn.kyuubi.session.SessionHandle
import yaooqinn.kyuubi.utils.NamedThreadFactory

/**
 * [[FrontendService]] keeps compatible with all kinds of Hive JDBC/Thrift Client Connections
 *
 * It use Hive configurations to configure itself.
 */
private[kyuubi] class FrontendService private(name: String, beService: BackendService)
  extends AbstractService(name) with TCLIService.Iface with Runnable with Logging {

  private[this] var hadoopConf: Configuration = _
  private[this] var authFactory: KyuubiAuthFactory = _

  private[this] val OK_STATUS = new TStatus(TStatusCode.SUCCESS_STATUS)

  private[this] var serverEventHandler: TServerEventHandler = _
  private[this] var currentServerContext: ThreadLocal[ServerContext] = _

  private[this] var server: Option[TServer] = _
  private[this] var portNum = 0
  private[this] var serverIPAddress: InetAddress = _

  private[this] val threadPoolName = name + "-Handler-Pool"

  private[this] var isStarted = false

  private[this] var realUser: String = _

  def this(beService: BackendService) = {
    this(classOf[FrontendService].getSimpleName, beService)
    currentServerContext = new ThreadLocal[ServerContext]()
    serverEventHandler = new FeTServerEventHandler
  }

  class FeServiceServerContext extends ServerContext {
    private var sessionHandle: SessionHandle = _

    def setSessionHandle(sessionHandle: SessionHandle): Unit = {
      this.sessionHandle = sessionHandle
    }

    def getSessionHandle: SessionHandle = sessionHandle
  }

  class FeTServerEventHandler extends TServerEventHandler {
    override def deleteContext(
        serverContext: ServerContext, tProtocol: TProtocol, tProtocol1: TProtocol): Unit = {
      Option(serverContext.asInstanceOf[FeServiceServerContext].getSessionHandle)
        .foreach { sessionHandle =>
          warn(s"Session [$sessionHandle] disconnected without closing properly, " +
          s"close it now")
          Try {beService.closeSession(sessionHandle)} match {
            case Failure(exception) =>
            warn("Failed closing session " + exception, exception)
            case _ =>
        }
      }
    }

    override def processContext(
        serverContext: ServerContext, tTransport: TTransport, tTransport1: TTransport): Unit = {
      currentServerContext.set(serverContext)
    }

    override def preServe(): Unit = ()

    override def createContext(tProtocol: TProtocol, tProtocol1: TProtocol): ServerContext = {
      new FeServiceServerContext()
    }
  }

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val serverHost = conf.get(FRONTEND_BIND_HOST.key)
    try {
      if (serverHost != null && !serverHost.isEmpty) {
        serverIPAddress = InetAddress.getByName(serverHost)
      } else {
        serverIPAddress = InetAddress.getLocalHost
      }
    } catch {
      case e: UnknownHostException => throw new ServiceException(e)
    }
    portNum = conf.get(FRONTEND_BIND_PORT.key).toInt
    super.init(conf)
  }

  override def start(): Unit = {
    super.start()
    if (!isStarted) {
      new Thread(this).start()
      isStarted = true
    }
  }

  override def stop(): Unit = {
    if (isStarted) {
      server.foreach(_.stop())
      info(this.name + " has stopped")
      isStarted = false
    }
    super.stop()
  }

  def getPortNumber: Int = portNum

  def getServerIPAddress: InetAddress = serverIPAddress

  private[this] def isKerberosAuthMode = {
    conf.get(KyuubiConf.AUTHENTICATION_METHOD.key).equalsIgnoreCase(AuthType.KERBEROS.name)
  }

  private[this] def getUserName(req: TOpenSessionReq) = {
    // Kerberos
    if (isKerberosAuthMode) {
      realUser = authFactory.getRemoteUser.orNull
    }
    // Except kerberos
    if (realUser == null) {
      realUser = TSetIpAddressProcessor.getUserName
    }
    if (realUser == null) {
      realUser = req.getUsername
    }
    realUser = getShortName(realUser)
    getProxyUser(req.getConfiguration.asScala.toMap, getIpAddress)
  }

  private[this] def getShortName(userName: String): String = {
    val indexOfDomainMatch = ServiceUtils.indexOfDomainMatch(userName)
    if (indexOfDomainMatch <= 0) {
      userName
    } else {
      userName.substring(0, indexOfDomainMatch)
    }
  }

  @throws[KyuubiSQLException]
  private[this] def getProxyUser(sessionConf: Map[String, String], ipAddress: String): String = {
    Option(sessionConf).flatMap(_.get(KyuubiAuthFactory.HS2_PROXY_USER)) match {
      case None => realUser
      case Some(_) if !conf.get(FRONTEND_ALLOW_USER_SUBSTITUTION.key).toBoolean =>
        throw new KyuubiSQLException("Proxy user substitution is not allowed")
      case Some(p) if !isKerberosAuthMode => p
      case Some(p) => // Verify proxy user privilege of the realUser for the proxyUser
        KyuubiAuthFactory.verifyProxyAccess(realUser, p, ipAddress, hadoopConf)
        p
    }
  }
  @throws[TTransportException]
  private[this] def getServerSocket(serverAddr: InetAddress, port: Int): TServerSocket = {
    try {
      val serverSocket = new ServerSocket(port, 1, serverAddr)
      this.portNum = serverSocket.getLocalPort
      new TServerSocket(serverSocket)
    } catch {
      case e: Exception => throw new ServiceException(e)
    }
  }

  private[this] def getIpAddress: String = {
    if (isKerberosAuthMode) {
      this.authFactory.getIpAddress.orNull
    } else {
      TSetIpAddressProcessor.getUserIpAddress
    }
  }

  private[this] def getMinVersion(versions: TProtocolVersion*): TProtocolVersion = {
    val values = TProtocolVersion.values
    var current = values(values.length - 1).getValue
    for (version <- versions) {
      if (current > version.getValue) {
        current = version.getValue
      }
    }
    for (version <- values) {
      if (version.getValue == current) {
        return version
      }
    }
    throw new IllegalArgumentException("never")
  }

  @throws[KyuubiSQLException]
  private[this] def getSessionHandle(req: TOpenSessionReq, res: TOpenSessionResp) = {
    val userName = getUserName(req)
    val ipAddress = getIpAddress
    val protocol = getMinVersion(BackendService.SERVER_VERSION, req.getClient_protocol)
    val sessionHandle =
    if (conf.get(FRONTEND_ENABLE_DOAS.key).toBoolean && (userName != null)) {
      beService.openSessionWithImpersonation(
        protocol, userName, req.getPassword, ipAddress, req.getConfiguration.asScala.toMap, null)
    } else {
      beService.openSession(
        protocol, userName, req.getPassword, ipAddress, req.getConfiguration.asScala.toMap)
    }
    res.setServerProtocolVersion(protocol)
    sessionHandle
  }

  override def OpenSession(req: TOpenSessionReq): TOpenSessionResp = {
    info("Client protocol version: " + req.getClient_protocol)
    val resp = new TOpenSessionResp
    try {
      val sessionHandle = getSessionHandle(req, resp)
      resp.setSessionHandle(sessionHandle.toTSessionHandle)
      // resp.setConfiguration(new Map[String, String]())
      resp.setStatus(OK_STATUS)
      val context = currentServerContext.get
        .asInstanceOf[FrontendService#FeServiceServerContext]
      if (context != null) {
        context.setSessionHandle(sessionHandle)
      }
    } catch {
      case e: Exception =>
        warn("Error opening session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CloseSession(req: TCloseSessionReq): TCloseSessionResp = {
    val resp = new TCloseSessionResp
    try {
      val sessionHandle = new SessionHandle(req.getSessionHandle)
      beService.closeSession(sessionHandle)
      resp.setStatus(OK_STATUS)
      val context = currentServerContext.get
        .asInstanceOf[FeServiceServerContext]
      if (context != null) {
        context.setSessionHandle(null)
      }
    } catch {
      case e: Exception =>
        warn("Error closing session: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetInfo(req: TGetInfoReq): TGetInfoResp = {
    val resp = new TGetInfoResp
    try {
      val getInfoValue = beService.getInfo(
        new SessionHandle(req.getSessionHandle), GetInfoType.getGetInfoType(req.getInfoType))
      resp.setInfoValue(getInfoValue.toTGetInfoValue)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting info: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def ExecuteStatement(req: TExecuteStatementReq): TExecuteStatementResp = {
    val resp = new TExecuteStatementResp
    try {
      val sessionHandle = new SessionHandle(req.getSessionHandle)
      val statement = req.getStatement
      val runAsync = req.isRunAsync
      val operationHandle = if (runAsync) {
        beService.executeStatementAsync(sessionHandle, statement)
      } else {
        beService.executeStatement(sessionHandle, statement)
      }
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error executing statement: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTypeInfo(req: TGetTypeInfoReq): TGetTypeInfoResp = {
    val resp = new TGetTypeInfoResp
    try {
      val operationHandle = beService.getTypeInfo(new SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(operationHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting type info: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetCatalogs(req: TGetCatalogsReq): TGetCatalogsResp = {
    val resp = new TGetCatalogsResp
    try {
      val opHandle = beService.getCatalogs(new SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting catalogs: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetSchemas(req: TGetSchemasReq): TGetSchemasResp = {
    val resp = new TGetSchemasResp
    try {
      val opHandle = beService.getSchemas(
        new SessionHandle(req.getSessionHandle), req.getCatalogName, req.getSchemaName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting schemas: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTables(req: TGetTablesReq): TGetTablesResp = {
    val resp = new TGetTablesResp
    try {
      val opHandle = beService.getTables(new SessionHandle(req.getSessionHandle),
        req.getCatalogName, req.getSchemaName, req.getTableName, req.getTableTypes.asScala)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting tables: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetTableTypes(req: TGetTableTypesReq): TGetTableTypesResp = {
    val resp = new TGetTableTypesResp
    try {
      val opHandle = beService.getTableTypes(new SessionHandle(req.getSessionHandle))
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting table types: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetColumns(req: TGetColumnsReq): TGetColumnsResp = {
    val resp = new TGetColumnsResp
    try {
      val opHandle = beService.getColumns(
        new SessionHandle(req.getSessionHandle),
        req.getCatalogName, req.getSchemaName, req.getTableName, req.getColumnName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting columns: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetFunctions(req: TGetFunctionsReq): TGetFunctionsResp = {
    val resp = new TGetFunctionsResp
    try {
      val opHandle = beService.getFunctions(
        new SessionHandle(req.getSessionHandle),
        req.getCatalogName, req.getSchemaName, req.getFunctionName)
      resp.setOperationHandle(opHandle.toTOperationHandle)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting functions: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetOperationStatus(req: TGetOperationStatusReq): TGetOperationStatusResp = {
    val resp = new TGetOperationStatusResp
    try {
      val operationStatus = beService.getOperationStatus(
        new OperationHandle(req.getOperationHandle))
      resp.setOperationState(operationStatus.getState.toTOperationState())
      val opException = operationStatus.getOperationException
      if (opException != null) {
        resp.setSqlState(opException.getSQLState)
        resp.setErrorCode(opException.getErrorCode)
        resp.setErrorMessage(opException.getMessage)
      }
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting operation status: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CancelOperation(req: TCancelOperationReq): TCancelOperationResp = {
    val resp = new TCancelOperationResp
    try {
      beService.cancelOperation(new OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error cancelling operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def CloseOperation(req: TCloseOperationReq): TCloseOperationResp = {
    val resp = new TCloseOperationResp
    try {
      beService.closeOperation(new OperationHandle(req.getOperationHandle))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error closing operation: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetResultSetMetadata(req: TGetResultSetMetadataReq): TGetResultSetMetadataResp = {
    val resp = new TGetResultSetMetadataResp
    try {
      val schema = beService.getResultSetMetadata(new OperationHandle(req.getOperationHandle))
      resp.setSchema(SchemaMapper.toTTableSchema(schema))
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error getting result set metadata: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def FetchResults(req: TFetchResultsReq): TFetchResultsResp = {
    val resp = new TFetchResultsResp
    try {
      val rowSet = beService.fetchResults(
        new OperationHandle(req.getOperationHandle),
        FetchOrientation.getFetchOrientation(req.getOrientation),
        req.getMaxRows,
        FetchType.getFetchType(req.getFetchType))
      resp.setResults(rowSet.toTRowSet)
      resp.setHasMoreRows(false)
      resp.setStatus(OK_STATUS)
    } catch {
      case e: Exception =>
        warn("Error fetching results: ", e)
        resp.setStatus(KyuubiSQLException.toTStatus(e))
    }
    resp
  }

  override def GetDelegationToken(req: TGetDelegationTokenReq): TGetDelegationTokenResp = {
    val resp = new TGetDelegationTokenResp
    if (authFactory == null) {
      resp.setStatus(notSupportTokenErrorStatus)
    } else {
      try {
        val token = beService.getDelegationToken(
          new SessionHandle(req.getSessionHandle),
          authFactory,
          req.getOwner,
          req.getRenewer)
        resp.setDelegationToken(token)
        resp.setStatus(OK_STATUS)
      } catch {
        case e: KyuubiSQLException =>
          error("Error obtaining delegation token", e)
          val tokenErrorStatus = KyuubiSQLException.toTStatus(e)
          tokenErrorStatus.setSqlState("42000")
          resp.setStatus(tokenErrorStatus)
      }
    }
    resp
  }

  private[this] def notSupportTokenErrorStatus = {
    val errorStatus = new TStatus(TStatusCode.ERROR_STATUS)
    errorStatus.setErrorMessage("Delegation token is not supported")
    errorStatus
  }

  override def CancelDelegationToken(req: TCancelDelegationTokenReq): TCancelDelegationTokenResp = {
    val resp = new TCancelDelegationTokenResp
    if (authFactory == null) {
      resp.setStatus(notSupportTokenErrorStatus)
    } else {
      try {
        beService.cancelDelegationToken(
          new SessionHandle(req.getSessionHandle),
          authFactory,
          req.getDelegationToken)
        resp.setStatus(OK_STATUS)
      } catch {
        case e: KyuubiSQLException =>
          error("Error canceling delegation token", e)
          resp.setStatus(KyuubiSQLException.toTStatus(e))
      }
    }
    resp
  }

  override def RenewDelegationToken(req: TRenewDelegationTokenReq): TRenewDelegationTokenResp = {
    val resp = new TRenewDelegationTokenResp
    if (authFactory == null) {
      resp.setStatus(notSupportTokenErrorStatus)
    } else {
      try {
        beService.renewDelegationToken(
          new SessionHandle(req.getSessionHandle),
          authFactory,
          req.getDelegationToken)
        resp.setStatus(OK_STATUS)
      } catch {
        case e: KyuubiSQLException =>
          error("Error obtaining renewing token", e)
          resp.setStatus(KyuubiSQLException.toTStatus(e))
      }
    }
    resp
  }

  override def run(): Unit = {
    try {
      // Server thread pool
      val minThreads = conf.get(FRONTEND_MIN_WORKER_THREADS.key).toInt
      val maxThreads = conf.get(FRONTEND_MAX_WORKER_THREADS.key).toInt
      val executorService = new ThreadPoolExecutor(
        minThreads,
        maxThreads,
        conf.getTimeAsSeconds(FRONTEND_WORKER_KEEPALIVE_TIME.key),
        TimeUnit.SECONDS,
        new SynchronousQueue[Runnable],
        new NamedThreadFactory(threadPoolName))

      // Thrift configs
      authFactory = new KyuubiAuthFactory(conf)
      val transportFactory = authFactory.getAuthTransFactory
      val processorFactory = authFactory.getAuthProcFactory(this)
      val serverSocket: TServerSocket = getServerSocket(serverIPAddress, portNum)

      // Server args
      val maxMessageSize = conf.get(FRONTEND_MAX_MESSAGE_SIZE.key).toInt
      val requestTimeout = conf.getTimeAsSeconds(FRONTEND_LOGIN_TIMEOUT.key).toInt
      val beBackoffSlotLength = conf.getTimeAsMs(FRONTEND_LOGIN_BEBACKOFF_SLOT_LENGTH.key).toInt
      val args = new TThreadPoolServer.Args(serverSocket)
        .processorFactory(processorFactory)
        .transportFactory(transportFactory)
        .protocolFactory(new TBinaryProtocol.Factory)
        .inputProtocolFactory(
          new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
        .requestTimeout(requestTimeout).requestTimeoutUnit(TimeUnit.SECONDS)
        .beBackoffSlotLength(beBackoffSlotLength)
        .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
        .executorService(executorService)
      // TCP Server
      server = Some(new TThreadPoolServer(args))
      server.foreach(_.setServerEventHandler(serverEventHandler))
      info(s"Starting $name on host ${serverIPAddress.getCanonicalHostName} at port $portNum with" +
        s" [$minThreads, $maxThreads] worker threads")
      server.foreach(_.serve())
    } catch {
      case t: Throwable =>
        error("Error starting " + name +  " for KyuubiServer", t)
        System.exit(-1)
    }
  }
}
