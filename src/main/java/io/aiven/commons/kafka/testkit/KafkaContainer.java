package io.aiven.commons.kafka.testkit;

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

import static org.assertj.core.api.Assertions.assertThat;
import static org.awaitility.Awaitility.await;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Properties;
import java.util.stream.Collectors;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.util.clusters.EmbeddedKafkaCluster;

/** A container for the schema registry. */
public final class KafkaContainer {
  /** The schema registry local port */
  public static final int KAFKA_PORT = 19092;

  public static final int CONTROLLER_KAFKA_PORT = 19093;
  private EmbeddedKafkaCluster kafka;
  private int numberofBrokers;

  /** Start a kafkaCluster */
  public KafkaContainer(int numBrokers, Properties properties) {
    kafka = new EmbeddedKafkaCluster(numBrokers, properties);
    kafka.start();
    this.numberofBrokers = numBrokers;
  }

  public boolean isReady() {
    return kafka.runningBrokers().size() == numberofBrokers;
  }

  public void createTopic(String topicName) {
    kafka.createTopic(topicName);
  }

  /** Start a kafkaCluster */
  public KafkaContainer(int numBrokers) {
    this(numBrokers, getDefaultProperties(19092));
  }

  /** Start a kafkaCluster */
  public KafkaContainer() {
    this(1, getDefaultProperties(19092));
  }

  /**
   * Get the schema registry URL for this container.
   *
   * @return the schema registry URL as a string.
   */
  public String getKafkaUrl() {
    return kafka.bootstrapServers();
  }

  public void stopCluster() {
    kafka.stop();
  }

  public KafkaConsumer<byte[], byte[]> kafkaConsumer() {
    return kafka.createConsumer(new HashMap<>());
  }

  public static Properties getDefaultProperties(int port) {
    Properties props = new Properties();
    props.put(
        "listeners",
        String.format("EXTERNAL://localhost:%d,CONTROLLER://localhost:%d", port, port + 1));
    props.put(
        "listener.security.protocol.map",
        "CONTROLLER:PLAINTEXT,EXTERNAL:PLAINTEXT,PLAINTEXT:PLAINTEXT");
    props.put("controller.listener.names", "CONTROLLER");
    props.put(
        "advertised.listeners",
        String.format("EXTERNAL://localhost:%d,CONTROLLER://localhost:%d", port, port + 1));
    props.put("inter.broker.listener.name", "EXTERNAL");
    return props;
  }

  /**
   * Read the data from the topic as byte array key and byte value. Each value is converted into a
   * string and returned in the result. If the expected number of messages is not read in the
   * allotted time the test fails.
   *
   * @param topic the topic to red.
   * @param expectedMessageCount the expected number of messages.
   * @param timeout the maximum time to wait for the messages to arrive.
   * @return A list of values returned.
   */
  public List<String> consumeByteMessages(
      final String topic, final int expectedMessageCount, final Duration timeout) {
    final Properties consumerProperties = consumerProperties();
    consumerProperties.put(
        ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    consumerProperties.put(
        ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    final List<ConsumerRecord<byte[], byte[]>> lst =
        consumeMessages(topic, consumerProperties, expectedMessageCount, timeout);
    return lst.stream()
        .map(cr -> new String(cr.value(), StandardCharsets.UTF_8))
        .collect(Collectors.toList());
  }

  /**
   * Read the data and key values from the topic. Teh consumer properties should be created with a
   * call to {@link #consumerProperties}. If the expected number of messages is not read in the
   * allotted time the test fails.
   *
   * @param topic the topic to red.
   * @param consumerProperties The consumer properties.
   * @param expectedMessageCount the expected number of messages.
   * @param timeout the maximum time to wait for the messages to arrive.
   * @param <X> The key type.
   * @param <V> the value type.
   * @return A list of values returned.
   */
  public <X, V> List<ConsumerRecord<X, V>> consumeMessages(
      final String topic,
      final Properties consumerProperties,
      final int expectedMessageCount,
      final Duration timeout) {
    try (KafkaConsumer<X, V> consumer = new KafkaConsumer<>(consumerProperties)) {
      consumer.subscribe(Collections.singletonList(topic));
      final List<ConsumerRecord<X, V>> recordValues = new ArrayList<>();
      await()
          .atMost(timeout)
          .pollInterval(Duration.ofSeconds(1))
          .untilAsserted(
              () -> {
                assertThat(consumeRecordsInProgress(consumer, recordValues))
                    .hasSize(expectedMessageCount);
              });
      return recordValues;
    }
  }

  /**
   * Consumes records in blocks and appends them to the {@code recordValues} parameter. This method
   * polls the consumer for 1/2 second and adds the result to the record values. As long as there
   * are at more than 10 records returned it continues to poll. Once there are fewer than 10 records
   * returned this mehtod returns.
   *
   * @param consumer The consumer to read from.
   * @param recordValues the record values to append to.
   * @return {@code recordValues}
   * @param <X> the key type.
   * @param <V> the value type.
   */
  private <X, V> List<ConsumerRecord<X, V>> consumeRecordsInProgress(
      final KafkaConsumer<X, V> consumer, final List<ConsumerRecord<X, V>> recordValues) {
    int recordsRetrieved;
    do {
      final ConsumerRecords<X, V> records = consumer.poll(Duration.ofMillis(500L));
      recordsRetrieved = records.count();
      records.forEach(recordValues::add);
      // Choosing 10 records as it allows for integration tests with a smaller max poll to be added
      // while maintaining efficiency, a slightly larger number could be added but this is slightly
      // more
      // efficient
      // than larger numbers.
    } while (recordsRetrieved > 10);
    return recordValues;
  }

  /**
   * Creates a Properties config with the proper bootstrap server.
   *
   * @return a Properties configured with the proper bootstrap server.
   */
  protected Properties consumerProperties() {
    Properties props = new Properties();
    props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-consumer-group");
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.bootstrapServers());

    return props;
  }
}
