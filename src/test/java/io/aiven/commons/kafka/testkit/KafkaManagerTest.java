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
import java.time.Duration;
import java.util.Collections;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;

public class KafkaManagerTest {

  private static KafkaManager underTest;

  @BeforeAll
  static void beforeAll() throws IOException {
    underTest = new KafkaManager("KafkaManagerTest", Duration.ofSeconds(2), Collections.emptyMap());
  }

  @AfterAll
  static void afterAll() throws IOException {
    if (underTest != null) {
      underTest.stop();
    }
  }

  @Test
  @Disabled("Schema registry does not work")
  void getSchemaRegistryTest() {
    underTest = underTest.withSchemaRegistry();
    assertThat(underTest.getSchemaRegistryUrl()).isNotNull();
  }
}
