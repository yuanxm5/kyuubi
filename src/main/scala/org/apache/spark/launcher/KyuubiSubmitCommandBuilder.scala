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

package org.apache.spark.launcher

import java.io.File
import java.util.{List => JList, Map => JMap}

import scala.collection.JavaConverters._

import org.apache.spark.launcher.CommandBuilderUtils._

class KyuubiSubmitCommandBuilder(args: JList[String]) extends SparkSubmitCommandBuilder(args) {

  override def buildCommand(env: JMap[String, String]): JList[String] = {
    val config = getEffectiveConfig
    val kyuubiJar = System.getenv("KYUUBI_JAR")
    val extraClassPath = config.get(SparkLauncher.DRIVER_EXTRA_CLASSPATH)
    val appClassPath = if (extraClassPath == null || extraClassPath.isEmpty) {
      kyuubiJar
    } else {
      kyuubiJar + File.pathSeparator + extraClassPath
    }

    val cmd = buildJavaCommand(appClassPath)

    val driverExtraJavaOptions = config.get(SparkLauncher.DRIVER_EXTRA_JAVA_OPTIONS)

    if (!isEmpty(driverExtraJavaOptions) && driverExtraJavaOptions.contains("Xmx")) {
      val msg =
        s"""
          | Not allowed to specify max heap(Xmx) memory settings through java options:
          | $driverExtraJavaOptions
          | Use the corresponding --driver-memory or spark.driver.memory configuration instead.
        """.stripMargin
      throw new IllegalArgumentException(msg)
    }

    val memory = firstNonEmpty(
      config.get(SparkLauncher.DRIVER_MEMORY),
      System.getenv("SPARK_DRIVER_MEMORY"),
      DEFAULT_MEM)
    cmd.add("-Xmx" + memory)
    addOptionString(cmd, driverExtraJavaOptions)
    mergeEnvPathList(env, getLibPathEnvName, config.get(SparkLauncher.DRIVER_EXTRA_LIBRARY_PATH))
    addPermSize(cmd)
    cmd.add("org.apache.spark.deploy.KyuubiSubmit")
    cmd.addAll(buildSparkSubmitArgs)
    cmd
  }

  /**
   * Adds the default perm gen size option for Spark if the VM requires it and the user hasn't
   * set it.
   */
  private def addPermSize(cmd: JList[String]): Unit = {
    // Don't set MaxPermSize for IBM Java, or Oracle Java 8 and later.
    if (getJavaVendor eq JavaVendor.IBM) {
      return
    }
    if (javaMajorVersion(System.getProperty("java.version")) > 7) {
      return
    }
    cmd.asScala.foreach { arg =>
      if (arg.contains("-XX:MaxPermSize=")) {
        return
      }
    }
    cmd.add("-XX:MaxPermSize=256m")
  }
}
