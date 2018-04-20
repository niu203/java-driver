/*
 * Copyright DataStax, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datastax.oss.driver.api.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.internal.core.cql.DefaultSimpleStatement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.NotThreadSafe;

@NotThreadSafe
public class SimpleStatementBuilder
    extends StatementBuilder<SimpleStatementBuilder, SimpleStatement> {

  private String query;
  private CqlIdentifier keyspace;
  private final List<Object> positionalValues = new ArrayList<>();
  private final Map<CqlIdentifier, Object> namedValues = new HashMap<>();

  public SimpleStatementBuilder(String query) {
    this.query = query;
  }

  public SimpleStatementBuilder(SimpleStatement template) {
    super(template);
    if (!template.getPositionalValues().isEmpty() && !template.getNamedValues().isEmpty()) {
      throw new IllegalArgumentException(
          "Illegal statement to copy, can't have both named and positional values");
    }

    this.query = template.getQuery();
    if (!template.getPositionalValues().isEmpty()) {
      this.positionalValues.addAll(template.getPositionalValues());
    }
    if (!template.getNamedValues().isEmpty()) {
      this.namedValues.putAll(template.getNamedValues());
    }
  }

  /** @see SimpleStatement#getQuery() */
  public SimpleStatementBuilder withQuery(String query) {
    this.query = query;
    return this;
  }

  /** @see SimpleStatement#getKeyspace() */
  public SimpleStatementBuilder withKeyspace(CqlIdentifier keyspace) {
    this.keyspace = keyspace;
    return this;
  }

  /**
   * Shortcut for {@link #withKeyspace(CqlIdentifier)
   * withKeyspace(CqlIdentifier.fromCql(keyspaceName))}.
   */
  public SimpleStatementBuilder withKeyspace(String keyspaceName) {
    return withKeyspace(CqlIdentifier.fromCql(keyspaceName));
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder addPositionalValue(Object value) {
    if (!namedValues.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    positionalValues.add(value);
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder addPositionalValues(Iterable<Object> values) {
    if (!namedValues.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    values.forEach(positionalValues::add);
    return this;
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder addPositionalValues(Object... values) {
    return addPositionalValues(Arrays.asList(values));
  }

  /** @see SimpleStatement#setPositionalValues(List) */
  public SimpleStatementBuilder clearPositionalValues() {
    positionalValues.clear();
    return this;
  }

  /** @see SimpleStatement#setNamedValuesWithIds(Map) */
  public SimpleStatementBuilder addNamedValue(CqlIdentifier name, Object value) {
    if (!positionalValues.isEmpty()) {
      throw new IllegalArgumentException(
          "Can't have both positional and named values in a statement.");
    }
    namedValues.put(name, value);
    return this;
  }

  /**
   * Shortcut for {@link #addNamedValue(CqlIdentifier, Object)
   * addNamedValue(CqlIdentifier.fromCql(name), value)}.
   */
  public SimpleStatementBuilder addNamedValue(String name, Object value) {
    return addNamedValue(CqlIdentifier.fromCql(name), value);
  }

  /** @see SimpleStatement#setNamedValuesWithIds(Map) */
  public SimpleStatementBuilder clearNamedValues() {
    namedValues.clear();
    return this;
  }

  @Override
  public SimpleStatement build() {
    return new DefaultSimpleStatement(
        query,
        positionalValues,
        namedValues,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        buildCustomPayload(),
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }
}
