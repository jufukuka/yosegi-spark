/**
 * Licensed to the Apache Software Foundation (ASF) under one or more contributor license
 * agreements. See the NOTICE file distributed with this work for additional information regarding
 * copyright ownership. The ASF licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License. You may obtain a
 * copy of the License at
 *
 * <p>http://www.apache.org/licenses/LICENSE-2.0
 *
 * <p>Unless required by applicable law or agreed to in writing, software distributed under the
 * License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing permissions and
 * limitations under the License.
 */
package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.LoadType;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkEmptyArrayLoader implements ILoader<WritableColumnVector> {

  private final WritableColumnVector vector;
  private final int loadSize;

  public SparkEmptyArrayLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
    this.vector.getChild(0).reset();
    this.vector.getChild(0).reserve(0);
    if (this.vector.getChild(0).hasDictionary()) {
      this.vector.getChild(0).reserveDictionaryIds(0);
      this.vector.getChild(0).setDictionary(null);
    }
  }

  @Override
  public LoadType getLoaderType() {
    return LoadType.NULL;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(final int index) throws IOException {
    // FIXME:
  }

  @Override
  public void finish() throws IOException {
    // FIXME:
  }

  @Override
  public WritableColumnVector build() throws IOException {
    for (int i = 0; i < loadSize; i++) {
      vector.putArray(i, 0, 0);
    }
    return vector;
  }

  @Override
  public boolean isLoadingSkipped() {
    return true;
  }
}