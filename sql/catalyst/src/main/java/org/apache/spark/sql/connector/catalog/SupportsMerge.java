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

package org.apache.spark.sql.connector.catalog;

import org.apache.spark.annotation.Evolving;
import org.apache.spark.sql.catalyst.expressions.Expression;
import org.apache.spark.sql.sources.Filter;

import java.util.List;
import java.util.Map;

/**
 * A mix-in interface for {@link Table} merge support. Data sources can implement this
 * interface to provide the ability to merge data from a data source table to a target
 * table that matches filter expressions.
 *
 * @since 3.0.0
 */
@Evolving
public interface SupportsMerge {

  void mergeIntoWithTable(
    SupportsMerge sourceTable,
    String targetAlias,
    String sourceTableName,
    String sourceAlias,
    Expression mergeCondition,
    Map<String, Expression> matchedActions,
    Map<String, Expression> notMatchedActions,
    Expression deleteExpression,
    Expression updateExpression,
    Expression insertExpression);

  void mergeIntoWithQuery(
    String targetAlias,
    String sourceTableName,
    String sourceAlias,
    Expression mergeCondition,
    Map<String, Expression> matchedActions,
    Map<String, Expression> notMatchedActions,
    Expression deleteExpression,
    Expression updateExpression,
    Expression insertExpression);
}