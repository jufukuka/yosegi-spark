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
package jp.co.yahoo.yosegi.spark.inmemory.factory;

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.inmemory.ILoaderFactory;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkConstNullArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkDictionaryNullArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkNullArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkNullLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkRunLengthEncodingArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkSequentialNullArrayLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkUnionArrayLoader;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkArrayLoaderFactory implements ILoaderFactory<WritableColumnVector> {
  private final WritableColumnVector vector;

  public SparkArrayLoaderFactory(final WritableColumnVector vector) {
    this.vector = vector;
  }

  @Override
  public ILoader createLoader(final ColumnBinary columnBinary, final int loadSize)
      throws IOException {
    if (columnBinary == null) {
      // FIXME:
      System.out.println("SparkArrayLoaderFactory: columnBinary is null");
      //return new SparkNullLoader(vector, loadSize);
      return new SparkNullArrayLoader(vector, loadSize);
    }
    System.out.println("SparkArrayLoaderFactory: columnBinary is " + columnBinary.columnType);
    switch (getLoadType(columnBinary, loadSize)) {
      case ARRAY:
        System.out.println("SparkArrayLoaderFactory:ARRAY");
        return new SparkArrayLoader(vector, loadSize);
      case RLE_ARRAY:
        System.out.println("SparkArrayLoaderFactory:RLE_ARRAY");
        return new SparkRunLengthEncodingArrayLoader(vector, loadSize);
      case UNION:
        System.out.println("SparkArrayLoaderFactory:UNION");
        return new SparkUnionArrayLoader(vector, loadSize);
      default:
        // FIXME:
        System.out.println("SparkArrayLoaderFactory: unknown load type");
        switch (getLoadType(columnBinary, loadSize)) {
          case SEQUENTIAL:
            System.out.println("SparkArrayLoaderFactory:SEQUENTIAL");
            return new SparkSequentialNullArrayLoader(vector, loadSize);
          case DICTIONARY:
            System.out.println("SparkArrayLoaderFactory:DICTIONARY");
            return new SparkDictionaryNullArrayLoader(vector, loadSize);
          case CONST:
            System.out.println("SparkArrayLoaderFactory:CONST");
            return new SparkConstNullArrayLoader(vector, loadSize);
          case UNION:
            System.out.println("SparkArrayLoaderFactory:UNION");
            break;
        }
        return new SparkNullLoader(vector, loadSize);
    }
  }
}
