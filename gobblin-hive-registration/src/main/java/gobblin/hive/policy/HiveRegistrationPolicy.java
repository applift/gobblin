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

package gobblin.hive.policy;

import java.io.IOException;

import org.apache.hadoop.fs.Path;

import gobblin.annotation.Alpha;
import gobblin.hive.HiveRegister;
import gobblin.hive.spec.HiveSpec;


/**
 * An interface for generating a {@link HiveSpec} for a {@link Path}.
 *
 * @author ziliu
 */
@Alpha
public interface HiveRegistrationPolicy {

  /**
   * Get a {@link HiveSpec} for a {@link Path}, which can be used by {@link HiveRegister}
   * to register the given {@link Path}.
   */
  public HiveSpec getHiveSpec(Path path) throws IOException;

}
