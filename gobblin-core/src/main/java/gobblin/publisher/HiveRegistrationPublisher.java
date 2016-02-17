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

package gobblin.publisher;

import java.io.IOException;
import java.util.Collection;
import java.util.Set;

import org.apache.hadoop.fs.Path;

import com.google.common.collect.Sets;
import com.google.common.io.Closer;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.configuration.WorkUnitState;
import gobblin.hive.HiveRegister;
import gobblin.hive.policy.HiveRegistrationPolicy;
import gobblin.hive.policy.HiveRegistrationPolicyBase;
import lombok.extern.slf4j.Slf4j;


/**
 * A {@link DataPublisher} that registers the already published data with Hive.
 *
 * <p>
 *   This publisher is not responsible for publishing data, and it relies on another publisher
 *   to document the published paths in property {@link ConfigurationKeys#PUBLISHER_DIRS}. Thus this publisher
 *   should generally be used as a job level data publisher, where the task level publisher should be a publisher
 *   that documents the published paths, such as {@link BaseDataPublisher}.
 * </p>
 *
 * @author ziliu
 */
@Slf4j
public class HiveRegistrationPublisher extends DataPublisher {

  private final Closer closer = Closer.create();
  private final HiveRegister hiveRegister;
  private final HiveRegistrationPolicy policy;

  public HiveRegistrationPublisher(State state) throws IOException {
    super(state);
    this.hiveRegister = this.closer.register(new HiveRegister(state));
    this.policy = HiveRegistrationPolicyBase.getPolicy(state);
  }

  @Override
  public void close() throws IOException {
    this.closer.close();
  }

  @Deprecated
  @Override
  public void initialize() throws IOException {
  }

  @Override
  public void publishData(Collection<? extends WorkUnitState> states) throws IOException {
    Set<String> pathsToRegister = getUniquePathsToRegister(states);
    log.info("Number of paths to be registered in Hive: " + pathsToRegister.size());
    for (String path : pathsToRegister) {
      this.hiveRegister.register(this.policy.getHiveSpec(new Path(path)));
    }
  }

  private Set<String> getUniquePathsToRegister(Collection<? extends WorkUnitState> states) {
    Set<String> paths = Sets.newHashSet();
    for (State state : states) {
      if (state.contains(ConfigurationKeys.PUBLISHER_DIRS)) {
        paths.addAll(state.getPropAsList(ConfigurationKeys.PUBLISHER_DIRS));
      }
    }
    return paths;
  }

  @Override
  public void publishMetadata(Collection<? extends WorkUnitState> states) throws IOException {
    // Nothing to do
  }

}
