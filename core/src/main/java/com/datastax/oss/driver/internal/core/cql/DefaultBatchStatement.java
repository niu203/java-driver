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
package com.datastax.oss.driver.internal.core.cql;

import com.datastax.oss.driver.api.core.CqlIdentifier;
import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.config.DriverConfigProfile;
import com.datastax.oss.driver.api.core.context.DriverContext;
import com.datastax.oss.driver.api.core.cql.BatchStatement;
import com.datastax.oss.driver.api.core.cql.BatchType;
import com.datastax.oss.driver.api.core.cql.BatchableStatement;
import com.datastax.oss.driver.api.core.cql.BoundStatement;
import com.datastax.oss.driver.api.core.cql.SimpleStatement;
import com.datastax.oss.driver.api.core.metadata.token.Token;
import com.datastax.oss.driver.api.core.time.ServerSideTimestampGenerator;
import com.datastax.oss.driver.api.core.type.codec.registry.CodecRegistry;
import com.datastax.oss.driver.shaded.guava.common.collect.ImmutableList;
import com.datastax.oss.driver.shaded.guava.common.collect.Iterables;
import com.datastax.oss.protocol.internal.PrimitiveSizes;
import java.nio.ByteBuffer;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import net.jcip.annotations.Immutable;

@Immutable
public class DefaultBatchStatement implements BatchStatement {

  private final BatchType batchType;
  private final List<BatchableStatement<?>> statements;
  private final String configProfileName;
  private final DriverConfigProfile configProfile;
  private final CqlIdentifier keyspace;
  private final CqlIdentifier routingKeyspace;
  private final ByteBuffer routingKey;
  private final Token routingToken;
  private final Map<String, ByteBuffer> customPayload;
  private final Boolean idempotent;
  private final boolean tracing;
  private final long timestamp;
  private final ByteBuffer pagingState;

  public DefaultBatchStatement(
      BatchType batchType,
      List<BatchableStatement<?>> statements,
      String configProfileName,
      DriverConfigProfile configProfile,
      CqlIdentifier keyspace,
      CqlIdentifier routingKeyspace,
      ByteBuffer routingKey,
      Token routingToken,
      Map<String, ByteBuffer> customPayload,
      Boolean idempotent,
      boolean tracing,
      long timestamp,
      ByteBuffer pagingState) {
    this.batchType = batchType;
    this.statements = ImmutableList.copyOf(statements);
    this.configProfileName = configProfileName;
    this.configProfile = configProfile;
    this.keyspace = keyspace;
    this.routingKeyspace = routingKeyspace;
    this.routingKey = routingKey;
    this.routingToken = routingToken;
    this.customPayload = customPayload;
    this.idempotent = idempotent;
    this.tracing = tracing;
    this.timestamp = timestamp;
    this.pagingState = pagingState;
  }

  @Override
  public BatchType getBatchType() {
    return batchType;
  }

  @Override
  public BatchStatement setBatchType(BatchType newBatchType) {
    return new DefaultBatchStatement(
        newBatchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public BatchStatement setKeyspace(CqlIdentifier newKeyspace) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        newKeyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public BatchStatement add(BatchableStatement<?> statement) {
    if (statements.size() >= 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    } else {
      return new DefaultBatchStatement(
          batchType,
          ImmutableList.<BatchableStatement<?>>builder().addAll(statements).add(statement).build(),
          configProfileName,
          configProfile,
          keyspace,
          routingKeyspace,
          routingKey,
          routingToken,
          customPayload,
          idempotent,
          tracing,
          timestamp,
          pagingState);
    }
  }

  @Override
  public BatchStatement addAll(Iterable<? extends BatchableStatement<?>> newStatements) {
    if (statements.size() + Iterables.size(newStatements) > 0xFFFF) {
      throw new IllegalStateException(
          "Batch statement cannot contain more than " + 0xFFFF + " statements.");
    } else {
      return new DefaultBatchStatement(
          batchType,
          ImmutableList.<BatchableStatement<?>>builder()
              .addAll(statements)
              .addAll(newStatements)
              .build(),
          configProfileName,
          configProfile,
          keyspace,
          routingKeyspace,
          routingKey,
          routingToken,
          customPayload,
          idempotent,
          tracing,
          timestamp,
          pagingState);
    }
  }

  @Override
  public int size() {
    return statements.size();
  }

  @Override
  public BatchStatement clear() {
    return new DefaultBatchStatement(
        batchType,
        ImmutableList.of(),
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Iterator<BatchableStatement<?>> iterator() {
    return statements.iterator();
  }

  @Override
  public ByteBuffer getPagingState() {
    return pagingState;
  }

  @Override
  public BatchStatement setPagingState(ByteBuffer newPagingState) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        newPagingState);
  }

  @Override
  public String getConfigProfileName() {
    return configProfileName;
  }

  @Override
  public BatchStatement setConfigProfileName(String newConfigProfileName) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        newConfigProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public DriverConfigProfile getConfigProfile() {
    return configProfile;
  }

  @Override
  public DefaultBatchStatement setConfigProfile(DriverConfigProfile newProfile) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        newProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public CqlIdentifier getKeyspace() {
    if (keyspace != null) {
      return keyspace;
    } else {
      for (BatchableStatement<?> statement : statements) {
        if (statement instanceof SimpleStatement && statement.getKeyspace() != null) {
          return statement.getKeyspace();
        }
      }
    }
    return null;
  }

  @Override
  public CqlIdentifier getRoutingKeyspace() {
    if (routingKeyspace != null) {
      return routingKeyspace;
    } else {
      for (BatchableStatement<?> statement : statements) {
        CqlIdentifier ks = statement.getRoutingKeyspace();
        if (ks != null) {
          return ks;
        }
      }
    }
    return null;
  }

  @Override
  public BatchStatement setRoutingKeyspace(CqlIdentifier newRoutingKeyspace) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        newRoutingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public ByteBuffer getRoutingKey() {
    if (routingKey != null) {
      return routingKey;
    } else {
      for (BatchableStatement<?> statement : statements) {
        ByteBuffer key = statement.getRoutingKey();
        if (key != null) {
          return key;
        }
      }
    }
    return null;
  }

  @Override
  public BatchStatement setRoutingKey(ByteBuffer newRoutingKey) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        newRoutingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Token getRoutingToken() {
    if (routingToken != null) {
      return routingToken;
    } else {
      for (BatchableStatement<?> statement : statements) {
        Token token = statement.getRoutingToken();
        if (token != null) {
          return token;
        }
      }
    }
    return null;
  }

  @Override
  public BatchStatement setRoutingToken(Token newRoutingToken) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        newRoutingToken,
        customPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Map<String, ByteBuffer> getCustomPayload() {
    return customPayload;
  }

  @Override
  public DefaultBatchStatement setCustomPayload(Map<String, ByteBuffer> newCustomPayload) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        newCustomPayload,
        idempotent,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public Boolean isIdempotent() {
    return idempotent;
  }

  @Override
  public DefaultBatchStatement setIdempotent(Boolean newIdempotence) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        newIdempotence,
        tracing,
        timestamp,
        pagingState);
  }

  @Override
  public boolean isTracing() {
    return tracing;
  }

  @Override
  public BatchStatement setTracing(boolean newTracing) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        newTracing,
        timestamp,
        pagingState);
  }

  @Override
  public long getTimestamp() {
    return timestamp;
  }

  @Override
  public BatchStatement setTimestamp(long newTimestamp) {
    return new DefaultBatchStatement(
        batchType,
        statements,
        configProfileName,
        configProfile,
        keyspace,
        routingKeyspace,
        routingKey,
        routingToken,
        customPayload,
        idempotent,
        tracing,
        newTimestamp,
        pagingState);
  }

  @Override
  public int computeSizeInBytes(DriverContext context) {

    int size = Conversions.minimumRequestSize(this, context);

    // BatchStatement's additional elements to take into account are:
    // - batch type
    // - inner statements (simple or bound)
    // - per-query keyspace
    // - timestamp

    // batch type
    size += PrimitiveSizes.BYTE;

    // inner statements
    size += PrimitiveSizes.SHORT; // number of statements
    for (BatchableStatement innerStatement : statements) {
      size +=
          sizeOfInnerBatchStatementInBytes(
              innerStatement, context.protocolVersion(), context.codecRegistry());
    }

    // per-query keyspace
    if (keyspace != null) {
      size += PrimitiveSizes.sizeOfString(keyspace.asInternal());
    }

    // timestamp
    if (!(context.timestampGenerator() instanceof ServerSideTimestampGenerator)
        || timestamp != Long.MIN_VALUE) {

      size += PrimitiveSizes.LONG;
    }

    return size;
  }

  /**
   * The size of a statement inside a batch query is different from the size of a complete
   * Statement. The inner batch statements only include the query or prepared ID, and the values of
   * the statement.
   */
  private Integer sizeOfInnerBatchStatementInBytes(
      BatchableStatement statement, ProtocolVersion protocolVersion, CodecRegistry codecRegistry) {
    int size = 0;

    size +=
        PrimitiveSizes
            .BYTE; // for each inner statement, there is one byte for the "kind": prepared or string

    if (statement instanceof SimpleStatement) {
      size += PrimitiveSizes.sizeOfLongString(((SimpleStatement) statement).getQuery());
      size +=
          Conversions.sizeOfSimpleStatementValues(
              ((SimpleStatement) statement), protocolVersion, codecRegistry);
    } else if (statement instanceof BoundStatement) {
      size +=
          PrimitiveSizes.sizeOfShortBytes(
              ((BoundStatement) statement).getPreparedStatement().getId().array());
      size += Conversions.sizeOfBoundStatementValues(((BoundStatement) statement));
    }
    return size;
  }
}
