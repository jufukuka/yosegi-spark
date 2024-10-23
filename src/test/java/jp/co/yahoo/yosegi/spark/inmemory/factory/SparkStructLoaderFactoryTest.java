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
import jp.co.yahoo.yosegi.binary.maker.DumpSpreadColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.ILoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkEmptyStructLoader;
import jp.co.yahoo.yosegi.spark.inmemory.loader.SparkStructLoader;
import jp.co.yahoo.yosegi.spark.test.Utils;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.params.provider.Arguments.arguments;

class SparkStructLoaderFactoryTest {
  public static Stream<Arguments> D_spreadColumnBinaryMaker() {
    return Stream.of(arguments(DumpSpreadColumnBinaryMaker.class.getName()));
  }

  @ParameterizedTest
  @MethodSource("D_spreadColumnBinaryMaker")
  void T_createLoader_Struct(final String binaryMakerClassName) throws IOException {
    final ColumnBinary columnBinary = Utils.dummyColumnBinary(binaryMakerClassName, false, false);

    final int loadSize = 5;
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("bi", DataTypes.BinaryType, true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final DataType dataType = DataTypes.createStructType(fields);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader =
        new SparkStructLoaderFactory(vector).createLoader(columnBinary, loadSize);

    assertTrue(loader instanceof SparkStructLoader);
  }

  @Test
  void T_createLoader_Null() throws IOException {
    final ColumnBinary columnBinary = null;

    final int loadSize = 5;
    final List<StructField> fields =
        Arrays.asList(
            DataTypes.createStructField("bo", DataTypes.BooleanType, true),
            DataTypes.createStructField("by", DataTypes.ByteType, true),
            DataTypes.createStructField("bi", DataTypes.BinaryType, true),
            DataTypes.createStructField("do", DataTypes.DoubleType, true),
            DataTypes.createStructField("fl", DataTypes.FloatType, true),
            DataTypes.createStructField("in", DataTypes.IntegerType, true),
            DataTypes.createStructField("lo", DataTypes.LongType, true),
            DataTypes.createStructField("sh", DataTypes.ShortType, true),
            DataTypes.createStructField("st", DataTypes.StringType, true));
    final DataType dataType = DataTypes.createStructType(fields);
    final WritableColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final ILoader loader =
        new SparkStructLoaderFactory(vector).createLoader(columnBinary, loadSize);

    assertTrue(loader instanceof SparkEmptyStructLoader);
  }
}
