/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package yaooqinn.kyuubi.operation

import org.apache.hive.service.cli.thrift.TOperationState

import yaooqinn.kyuubi.KyuubiSQLException

trait OperationState {
  def toTOperationState(): TOperationState
  def isTerminal(): Boolean = false

  @throws[KyuubiSQLException]
  def validateTransition(newState: OperationState): Unit = ex(newState)

  @throws[KyuubiSQLException]
  protected def ex(state: OperationState): Unit = throw new KyuubiSQLException(
    "Illegal Operation state transition " + this + " -> " + state, "ServerError", 1000)
}

case object INITIALIZED extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.INITIALIZED_STATE
  override def validateTransition(newState: OperationState): Unit = newState match {
    case PENDING | RUNNING | CANCELED | CLOSED =>
    case _ => ex(newState)
  }
}

case object RUNNING extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.RUNNING_STATE
  override def validateTransition(newState: OperationState): Unit = newState match {
    case FINISHED | CANCELED | ERROR | CLOSED =>
    case _ => ex(newState)
  }
}

case object FINISHED extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.FINISHED_STATE
  override def isTerminal(): Boolean = true
  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object CANCELED extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.CANCELED_STATE
  override def isTerminal(): Boolean = true
  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object CLOSED extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.CLOSED_STATE
  override def isTerminal(): Boolean = true
}

case object ERROR extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.ERROR_STATE
  override def isTerminal(): Boolean = true
  override def validateTransition(newState: OperationState): Unit = newState match {
    case CLOSED =>
    case _ => ex(newState)
  }
}

case object UNKNOWN extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.UKNOWN_STATE
}

case object PENDING extends OperationState {
  override def toTOperationState(): TOperationState = TOperationState.PENDING_STATE
  override def validateTransition(newState: OperationState): Unit = newState match {
    case RUNNING | FINISHED | CANCELED | ERROR | CLOSED =>
    case _ => ex(newState)
  }
}
