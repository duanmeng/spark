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

package org.apache.spark.sql.connector.write;

import org.apache.spark.annotation.Experimental;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.connector.catalog.Table;
import org.apache.spark.sql.sources.Filter;

import java.util.Map;
import java.util.Optional;

/**
 * A mix-in interface for {@link Table} update support. Data sources can implement this
 * interface to provide the ability to update data from tables that matches filter expressions.
 */
@Experimental
public interface SupportsUpdate {
  /**
   * Update data from a data source table that matches filter expressions.
   * <p>
   * Rows are updated from the data source iff all of the filter expressions match. That is, the
   * expressions must be interpreted as a set of filters that are ANDed together.
   * <p>
   * Implementations may reject a update operation if the update isn't possible without significant
   * effort. For example, partitioned data sources may reject updates that do not filter by
   * partition columns because the filter may require rewriting files without updated records.
   * To reject a update implementations should throw {@link IllegalArgumentException} with a clear
   * error message that identifies which expression was rejected.
   *
   * @throws IllegalArgumentException If the update is rejected due to required effort
   */
  void update(Map<String, Expression> assignments, Expression updateExpression);
}
