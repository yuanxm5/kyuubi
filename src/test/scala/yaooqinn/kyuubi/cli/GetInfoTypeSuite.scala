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

package yaooqinn.kyuubi.cli

import org.apache.hive.service.cli.thrift.TGetInfoType
import org.apache.spark.SparkFunSuite

class GetInfoTypeSuite extends SparkFunSuite {

  test("Get Info Type basic tests") {
    assert(GetInfoType.getGetInfoType(TGetInfoType.CLI_DBMS_NAME) === GetInfoType.DBMS_NAME)
    assert(GetInfoType.getGetInfoType(TGetInfoType.CLI_DBMS_VER) === GetInfoType.DBMS_VERSION)
    assert(GetInfoType.getGetInfoType(TGetInfoType.CLI_SERVER_NAME) === GetInfoType.SERVER_NAME)

    intercept[IllegalArgumentException](
      GetInfoType.getGetInfoType(TGetInfoType.CLI_ACCESSIBLE_PROCEDURES))
  }

}
