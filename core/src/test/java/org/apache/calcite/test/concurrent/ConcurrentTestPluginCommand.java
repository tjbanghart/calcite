/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.calcite.test.concurrent;

import org.checkerframework.checker.nullness.qual.Nullable;

import java.sql.Connection;
import java.sql.Statement;

/**
 * Used to extend functionality of mtsql.
 */
public interface ConcurrentTestPluginCommand {

  /** Test context. */
  interface TestContext {
    /**
     * Store a message as output for mtsql script.
     *
     * @param message Message to be output
     */
    void storeMessage(String message);

    /**
     * Get connection for thread.
     *
     * @return connection for thread
     */
    Connection getConnection();

    /**
     * Get current statement for thread, or null if none.
     *
     * @return current statement for thread
     */
    @Nullable Statement getCurrentStatement();
  }

  /**
   * Implement this method to extend functionality of mtsql.
   *
   * @param testContext Exposed context for plugin to run in.
   */
  void execute(TestContext testContext);
}
