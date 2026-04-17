/*
        Copyright 2026 Aiven Oy and project contributors

       Licensed under the Apache License, Version 2.0 (the "License");
       you may not use this file except in compliance with the License.
       You may obtain a copy of the License at

       https://www.apache.org/licenses/LICENSE-2.0

       Unless required by applicable law or agreed to in writing,
       software distributed under the License is distributed on an
       "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
       KIND, either express or implied.  See the License for the
       specific language governing permissions and limitations
       under the License.

       SPDX-License-Identifier: Apache-2.0
*/
package io.aiven.commons.kafka.testkit;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.util.Properties;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

public class KafkaContainerTest {

  private static KafkaContainer underTest = null;

  @BeforeAll
  static void beforeAll() throws IOException {
    Properties props = new Properties();
    props.put("listeners", "EXTERNAL://localhost:19092,CONTROLLER://localhost:19093");
    props.put(
        "listener.security.protocol.map",
        "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT");
    props.put("controller.listener.names", "CONTROLLER");
    props.put("advertised.listeners", "EXTERNAL://localhost:19092,CONTROLLER://localhost:19093");
    props.put("inter.broker.listener.name", "EXTERNAL");
    underTest = new KafkaContainer(1, props);
    //    underTest.startConnectCluster("KafkaConnectRunnerTest", Collections.emptyMap());
  }

  @AfterAll
  static void afterAll() throws IOException {
    //    underTest.stopConnectCluster();
  }

  @Test
  void simpleRetrievalTests() {
    assertThat(underTest.getKafkaUrl()).isEqualTo("localhost:19092");
  }
}
