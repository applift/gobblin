/*
 * Copyright (C) 2014-2016 LinkedIn Corp. All rights reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied.
 */

package gobblin.source.extractor.extract.kafka;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentMap;
import java.util.regex.Pattern;

import org.apache.commons.lang3.NotImplementedException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.net.HostAndPort;
import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.util.ConfigUtils;
import gobblin.util.DatasetFilterUtils;
import kafka.api.PartitionFetchInfo;
import kafka.api.PartitionOffsetRequestInfo;
import kafka.common.TopicAndPartition;
import kafka.javaapi.FetchRequest;
import kafka.javaapi.FetchResponse;
import kafka.javaapi.OffsetRequest;
import kafka.javaapi.OffsetResponse;
import kafka.javaapi.PartitionMetadata;
import kafka.javaapi.TopicMetadata;
import kafka.javaapi.TopicMetadataRequest;
import kafka.javaapi.consumer.SimpleConsumer;
import kafka.javaapi.message.ByteBufferMessageSet;
import kafka.message.MessageAndOffset;


/**
 * Wrapper class that contains two alternative Kakfa APIs: an old low-level Scala-based API, and a new API.
 * The new API has not been implemented since it's not ready to be open sourced.
 *
 * @author Ziyang Liu
 */
public class KafkaWrapper implements Closeable {

  private static final Logger LOG = LoggerFactory.getLogger(KafkaWrapper.class);

  private static final String USE_NEW_KAFKA_API = "use.new.kafka.api";
  private static final boolean DEFAULT_USE_NEW_KAFKA_API = false;

  private final List<String> brokers;
  private final KafkaAPI kafkaAPI;

  private final boolean useNewKafkaAPI;

  private static class Builder {
    private boolean useNewKafkaAPI = DEFAULT_USE_NEW_KAFKA_API;
    private List<String> brokers = Lists.newArrayList();
    private Config config = ConfigFactory.empty();

    private Builder withNewKafkaAPI() {
      this.useNewKafkaAPI = true;
      return this;
    }

    private Builder withBrokers(List<String> brokers) {
      for (String broker : brokers) {
        Preconditions.checkArgument(broker.matches(".+:\\d+"),
            String.format("Invalid broker: %s. Must be in the format of address:port.", broker));
      }
      this.brokers = Lists.newArrayList(brokers);
      return this;
    }

    private Builder withConfig(Config config) {
      this.config = config;
      return this;
    }

    private KafkaWrapper build() {
      Preconditions.checkArgument(!this.brokers.isEmpty(), "Need to specify at least one Kafka broker.");
      return new KafkaWrapper(this);
    }
  }

  private KafkaWrapper(Builder builder) {
    this.useNewKafkaAPI = builder.useNewKafkaAPI;
    this.brokers = builder.brokers;
    this.kafkaAPI = getKafkaAPI(builder.config);
  }

  /**
   * Create a KafkaWrapper based on the given type of Kafka API and list of Kafka brokers.
   *
   * @param state A {@link State} object that should contain a list of comma separated Kafka brokers
   * in property "kafka.brokers". It may optionally specify whether to use the new Kafka API by setting
   * use.new.kafka.api=true.
   */
  public static KafkaWrapper create(State state) {
    Preconditions.checkNotNull(state.getProp(ConfigurationKeys.KAFKA_BROKERS),
        "Need to specify at least one Kafka broker.");
    KafkaWrapper.Builder builder = new KafkaWrapper.Builder();
    if (state.getPropAsBoolean(USE_NEW_KAFKA_API, DEFAULT_USE_NEW_KAFKA_API)) {
      builder = builder.withNewKafkaAPI();
    }
    Config config = ConfigUtils.propertiesToConfig(state.getProperties());
    return builder.withBrokers(state.getPropAsList(ConfigurationKeys.KAFKA_BROKERS))
        .withConfig(config)
        .build();
  }

  public List<String> getBrokers() {
    return this.brokers;
  }

  public List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist) {
    return this.kafkaAPI.getFilteredTopics(blacklist, whitelist);
  }

  public long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    return this.kafkaAPI.getEarliestOffset(partition);
  }

  public long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
    return this.kafkaAPI.getLatestOffset(partition);
  }

  public Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset, long maxOffset) {
    return this.kafkaAPI.fetchNextMessageBuffer(partition, nextOffset, maxOffset);
  }

  private KafkaAPI getKafkaAPI(Config config) {
    if (this.useNewKafkaAPI) {
      return new KafkaNewAPI(config);
    }
    return new KafkaOldAPI(config);
  }

  @Override
  public void close() throws IOException {
    this.kafkaAPI.close();
  }

  private abstract class KafkaAPI implements Closeable {

    protected KafkaAPI(Config config) {
    }

    protected abstract List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist);

    protected abstract long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException;

    protected abstract long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException;

    protected abstract Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset,
        long maxOffset);
  }

  /**
   * Wrapper for the old low-level Scala-based Kafka API.
   */
  private class KafkaOldAPI extends KafkaAPI {
    public static final String CONFIG_PREFIX = "source.kafka.";
    public static final String CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE = CONFIG_PREFIX + "socketTimeoutMillis";
    public static final int CONFIG_KAFKA_SOCKET_TIMEOUT_VALUE_DEFAULT = 30000; // 30 seconds
    public static final String CONFIG_KAFKA_BUFFER_SIZE_BYTES = CONFIG_PREFIX + "bufferSizeBytes";
    public static final int CONFIG_KAFKA_BUFFER_SIZE_BYTES_DEFAULT = 1024*1024*10; // 1MB
    public static final String CONFIG_KAFKA_CLIENT_NAME = CONFIG_PREFIX + "clientName";
    public static final String CONFIG_KAFKA_CLIENT_NAME_DEFAULT = "gobblin-kafka";
    public static final String CONFIG_KAFKA_FETCH_REQUEST_CORRELATION_ID = CONFIG_PREFIX + "fetchCorrelationId";
    private static final int CONFIG_KAFKA_FETCH_REQUEST_CORRELATION_ID_DEFAULT = -1;
    public static final String CONFIG_KAFKA_FETCH_TIMEOUT_VALUE = CONFIG_PREFIX + "fetchTimeoutMillis";
    public static final int CONFIG_KAFKA_FETCH_TIMEOUT_VALUE_DEFAULT = 1000; // 1 second
    public static final String CONFIG_KAFKA_FETCH_REQUEST_MIN_BYTES = CONFIG_PREFIX + "fetchMinBytes";
    private static final int CONFIG_KAFKA_FETCH_REQUEST_MIN_BYTES_DEFAULT = 1024;
    public static final String CONFIG_KAFKA_FETCH_TOPIC_NUM_TRIES = CONFIG_PREFIX + "fetchTopicNumTries";
    private static final int CONFIG_KAFKA_FETCH_TOPIC_NUM_TRIES_DEFAULT = 3;
    public static final String CONFIG_KAFKA_FETCH_OFFSET_NUM_TRIES = CONFIG_PREFIX + "fetchOffsetNumTries";
    private static final int CONFIG_KAFKA_FETCH_OFFSET_NUM_TRIES_DEFAULT = 3;

    private final int socketTimeoutMillis;
    private final int bufferSize;
    private final String clientName;
    private final int fetchCorrelationId;
    private final int fetchTimeoutMillis;
    private final int fetchMinBytes;
    private final int fetchTopicRetries;
    private final int fetchOffsetRetries;
  }

  /**
   * Wrapper for the new Kafka API.
   */
  private class KafkaNewAPI extends KafkaAPI {

    protected KafkaNewAPI(Config config) {
      super(config);
    }

    @Override
    public List<KafkaTopic> getFilteredTopics(List<Pattern> blacklist, List<Pattern> whitelist) {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    protected long getEarliestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    protected long getLatestOffset(KafkaPartition partition) throws KafkaOffsetRetrievalFailureException {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    public void close() throws IOException {
      throw new NotImplementedException("kafka new API has not been implemented");
    }

    @Override
    protected Iterator<MessageAndOffset> fetchNextMessageBuffer(KafkaPartition partition, long nextOffset,
        long maxOffset) {
      throw new NotImplementedException("kafka new API has not been implemented");
    }
  }

}
