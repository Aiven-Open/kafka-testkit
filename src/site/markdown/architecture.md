<!--
 Copyright Aiven Oy and project contributors.

 Licensed under the Apache License, Version 2.0 (the "License");
 you may not use this file except in compliance with the License.
 You may obtain a copy of the License at

 https://www.apache.org/licenses/LICENSE-2.0

 Unless required by applicable law or agreed to in writing, software
 distributed under the License is distributed on an "AS IS" BASIS,
 WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 See the License for the specific language governing permissions and
 limitations under the License.

     SPDX-License-Identifier: Apache-2.0
-->
# Architecture

The project is designed to make executing Kafka connect tests easier.  It also has applications for any java project that needs to start a Kafka system for verification.

There are four basic components

```
               /--------------------------\
               | KafkaIntegrationTestBase |
               \-------------+------------/
                             | (uses)
                             V
                      /---------------\          
                      | Kafka Manager |
                      \------+--------/
                             |
            +----------------+-----------------+
            |                                  |
            V                                  V
   /--------------------\          /-------------------------\
   | KafkaConnectRunner |          | SchemaRegistryContainer |                                     
   \--------------------/          \-------------------------/
   
   ```

## KafkaConnectRunner

The KafkaConnectRunner manages a kafka cluster.  The connect runner creates and manages an Apache Kafka EmbeddedConnectCluster.  The runner manages the discovery of open ports to use for the embedded cluster and provides simple getters to retrieve the most commonly used values from the cluster. It also has methods to set values that can be changed while the cluster is running as well as stopping, starting, pausing and resuming the execution of the connector.

## SchemaRegistryContainer

This is, as it says, a container of a [Karapace](https://github.com/Aiven-Open/karapace) Schema Registry.  This is a generic TestContainer and may be used wherever java TestContainers are supported.  The container provides a getter to retrieve the schema registry url.

## KafkaManager

The Kafka manager configures a KafkaConnectRunner and SchemaRegistryContainer.  It provides getter methods that wrap the getters in the connect runner and schema registry container as well as creating topics.

## KafkaIntegrationTestBase

This class provides an abstract base for integration tests that require the KafkaConnectManager. The test base runs a KafkaManager in a ThreadLocal variable.  This allows multiple integration tests to run simultaneously provided there is sufficient memory.  The name of the container is also stored in a ThreadLocal variable.  This class also  tracks the TestInfo that is passed in from the Junit framework and makes this value available to all tests.  The class provides the following methods:

 * getTopic() - Returns the a topic name based on the test method being executed.  Also allows for additional segments so that a topic may be created for each test even if that test is parameter driven.
 * setupKafka() - Allows the test to start kafka and retrieve a handle to the Kafka manager.  This method will only setup Kafka if the kafka is not already running or the `force` parameter has been set.  This means that all tests may startup the kafka and they will not all incur the startup overhead.  The use of individual topics via the `getTopic()` method ensures that the data will not collide.  The cluster name is the test case name as provided by the TestInfo converted from camel case to kebab case.
 * getKafkaManager() - Retrieve the handle to the running Kafka but will fail if `setupKafka()` was not called at some earlier point.
 * waitForStorage() - Most connector tests require data to be written to some sort of storage.  This method will wait for the storage to report that a list of items are available or until a timer expires.





