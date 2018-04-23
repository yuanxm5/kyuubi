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

package yaooqinn.kyuubi.session

import java.io.{File, IOException}
import java.util.Date
import java.util.concurrent._
import java.util.concurrent.atomic.AtomicInteger

import scala.collection.JavaConverters._

import org.apache.commons.io.FileUtils
import org.apache.hive.service.cli.thrift.TProtocolVersion
import org.apache.spark.KyuubiConf._
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

import yaooqinn.kyuubi.{KyuubiSQLException, Logging}
import yaooqinn.kyuubi.operation.OperationManager
import yaooqinn.kyuubi.server.KyuubiServer
import yaooqinn.kyuubi.service.CompositeService
import yaooqinn.kyuubi.spark.SparkSessionCacheManager
import yaooqinn.kyuubi.ui.KyuubiServerMonitor
import yaooqinn.kyuubi.utils.NamedThreadFactory

/**
 * A SessionManager for managing [[KyuubiSession]]s
 */
private[kyuubi] class SessionManager private(
    name: String) extends CompositeService(name) with Logging {
  private[this] val operationManager = new OperationManager()
  private[this] val handleToSession = new ConcurrentHashMap[SessionHandle, KyuubiSession]
  private[this] var execPool: ThreadPoolExecutor = _
  private[this] var isOperationLogEnabled = false
  private[this] var operationLogRootDir: File = _
  private[this] var checkInterval: Long = _
  private[this] var sessionTimeout: Long = _
  private[this] var checkOperation: Boolean = false
  private[this] var shutdown: Boolean = false

  def this() = this(classOf[SessionManager].getSimpleName)

  private[this] def createExecPool(): Unit = {
    val poolSize = conf.get(ASYNC_EXEC_THREADS.key).toInt
    info("Background operation thread pool size: " + poolSize)
    val poolQueueSize = conf.get(ASYNC_EXEC_WAIT_QUEUE_SIZE.key).toInt
    info("Background operation thread wait queue size: " + poolQueueSize)
    val keepAliveTime = conf.getTimeAsSeconds(EXEC_KEEPALIVE_TIME.key)
    info("Background operation thread keepalive time: " + keepAliveTime + " seconds")
    val threadPoolName = classOf[KyuubiServer].getSimpleName + "-Background-Pool"
    execPool =
      new ThreadPoolExecutor(
        poolSize,
        poolSize,
        keepAliveTime,
        TimeUnit.SECONDS,
        new LinkedBlockingQueue[Runnable](poolQueueSize),
        new NamedThreadFactory(threadPoolName))
    execPool.allowCoreThreadTimeOut(true)
    checkInterval = conf.getTimeAsMs(FRONTEND_SESSION_CHECK_INTERVAL.key)
    sessionTimeout = conf.getTimeAsMs(FRONTEND_IDLE_SESSION_TIMEOUT.key)
    checkOperation = conf.get(FRONTEND_IDLE_SESSION_CHECK_OPERATION.key).toBoolean
  }

  private[this] def initOperationLogRootDir(): Unit = {
    operationLogRootDir = new File(conf.get(LOGGING_OPERATION_LOG_DIR.key))
    isOperationLogEnabled = true
    if (operationLogRootDir.exists && !operationLogRootDir.isDirectory) {
      info("The operation log root directory exists, but it is not a directory: "
        + operationLogRootDir.getAbsolutePath)
      isOperationLogEnabled = false
    }
    if (!operationLogRootDir.exists) {
      if (!operationLogRootDir.mkdirs) {
        warn("Unable to create operation log root directory: "
          + operationLogRootDir.getAbsolutePath)
        isOperationLogEnabled = false
      }
    }

    if (isOperationLogEnabled) {
      if (operationLogRootDir.exists()) {
        info("Operation log root directory is created: " + operationLogRootDir.getAbsolutePath)
        try {
          FileUtils.forceDeleteOnExit(operationLogRootDir)
        } catch {
          case e: IOException =>
            warn("Failed to schedule cleanup Spark Thrift Server's operation logging root" +
              " dir: " + operationLogRootDir.getAbsolutePath, e)
        }
      } else {
        warn("Operation log root directory is not created: "
          + operationLogRootDir.getAbsolutePath)
      }
    }
  }

  /**
   * Periodically close idle sessions in 'spark.kyuubi.frontend.session.check.interval(default 6h)'
   */
  private[this] def startTimeoutChecker(): Unit = {
    val interval: Long = math.max(checkInterval, 3000L)
    // minimum 3 seconds
    val timeoutChecker = new Runnable() {
      override def run(): Unit = {
        sleepInterval(interval)
        while (!shutdown) {
          val current: Long = System.currentTimeMillis
          handleToSession.values.asScala.foreach { session =>
            if (sessionTimeout > 0 && session.getLastAccessTime + sessionTimeout <= current
              && (!checkOperation || session.getNoOperationTime > sessionTimeout)) {
              val handle: SessionHandle = session.getSessionHandle
              warn("Session " + handle + " is Timed-out (last access: "
                + new Date(session.getLastAccessTime) + ") and will be closed")
              try {
                closeSession(handle)
              } catch {
                case e: KyuubiSQLException =>
                  warn("Exception is thrown closing idle session " + handle, e)
              }
            } else {
              session.closeExpiredOperations
            }
          }
          sleepInterval(interval)
        }
      }
    }
    execPool.execute(timeoutChecker)
  }

  private[this] def sleepInterval(interval: Long): Unit = {
    try {
      Thread.sleep(interval)
    } catch {
      case _: InterruptedException =>
      // ignore
    }
  }

  private[this] def cleanupLoggingRootDir(): Unit = {
    if (isOperationLogEnabled) {
      try {
        FileUtils.forceDelete(operationLogRootDir)
      } catch {
        case e: Exception =>
          warn("Failed to cleanup root dir of KyuubiServer logging: "
            + operationLogRootDir.getAbsolutePath, e)
      }
    }
  }

  override def init(conf: SparkConf): Unit = synchronized {
    this.conf = conf
    // Create operation log root directory, if operation logging is enabled
    if (conf.get(LOGGING_OPERATION_ENABLED.key).toBoolean) {
      initOperationLogRootDir()
    }
    createExecPool()
    addService(operationManager)
    super.init(conf)
  }

  override def start(): Unit = {
    super.start()
    if (checkInterval > 0) {
      startTimeoutChecker()
    }
    SparkSessionCacheManager.startCacheManager(conf)
  }

  override def stop(): Unit = {
    super.stop()
    shutdown = true
    if (execPool != null) {
      execPool.shutdown()
      val timeout = conf.getTimeAsSeconds(ASYNC_EXEC_SHUTDOWN_TIMEOUT.key)
      try {
        execPool.awaitTermination(timeout, TimeUnit.SECONDS)
      } catch {
        case e: InterruptedException =>
          warn("ASYNC_EXEC_SHUTDOWN_TIMEOUT = " + timeout +
            " seconds has been exceeded. RUNNING background operations will be shut down", e)
      }
      execPool = null
    }
    cleanupLoggingRootDir()
    SparkSessionCacheManager.get.stop()
  }

  /**
   * Opens a new session and creates a session handle.
   * The username passed to this method is the effective username.
   * If withImpersonation is true (==doAs true) we wrap all the calls in HiveSession
   * within a UGI.doAs, where UGI corresponds to the effective user.
   */
  @throws[KyuubiSQLException]
  def openSession(
      protocol: TProtocolVersion,
      username: String,
      password: String,
      ipAddress: String,
      sessionConf: Map[String, String],
      withImpersonation: Boolean): SessionHandle = {
    val kyuubiSession = new KyuubiSession(
      protocol,
      username,
      password,
      conf.clone(),
      ipAddress,
      withImpersonation,
      this,
      operationManager)
    info(s"Opening session for $username")
    kyuubiSession.open(sessionConf)

    if (isOperationLogEnabled) {
      kyuubiSession.setOperationLogSessionDir(operationLogRootDir)
    }

    val sessionHandle = kyuubiSession.getSessionHandle
    handleToSession.put(sessionHandle, kyuubiSession)
    KyuubiServerMonitor.getListener(username).foreach {
      _.onSessionCreated(
        kyuubiSession.getIpAddress,
        sessionHandle.getSessionId.toString,
        kyuubiSession.getUserName)
    }

    sessionHandle
  }

  @throws[KyuubiSQLException]
  def getSession(sessionHandle: SessionHandle): KyuubiSession = {
    val session = handleToSession.get(sessionHandle)
    if (session == null) {
      throw new KyuubiSQLException("Invalid SessionHandle: " + sessionHandle)
    }
    session
  }

  @throws[KyuubiSQLException]
  def closeSession(sessionHandle: SessionHandle) {
    val session = handleToSession.remove(sessionHandle)
    if (session == null) {
      throw new KyuubiSQLException(s"Session $sessionHandle does not exist!")
    }
    val sessionUser = session.getUserName
    KyuubiServerMonitor.getListener(sessionUser).foreach {
      _.onSessionClosed(sessionHandle.getSessionId.toString)
    }
    SparkSessionCacheManager.get.decrease(sessionUser)
    session.close()
  }

  def getOperationMgr: OperationManager = operationManager

  def getOpenSessionCount: Int = handleToSession.size

  def submitBackgroundOperation(r: Runnable): Future[_] = execPool.submit(r)
}
