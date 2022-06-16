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

import jp.co.yahoo.yosegi.binary.ColumnBinary;
import jp.co.yahoo.yosegi.binary.maker.IColumnBinaryMaker;
import jp.co.yahoo.yosegi.inmemory.IUnionLoader;
import jp.co.yahoo.yosegi.spark.test.UnionColumnUtils;
import jp.co.yahoo.yosegi.spark.test.Utils;
import jp.co.yahoo.yosegi.spread.column.ColumnType;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

class SparkUnionDecimalLoaderTest {
  public static <T> void assertDecimal(
      final Map<Integer, T> values, final WritableColumnVector vector, int loadSize, int precision, int scale) {
    for (final Integer i : values.keySet()) {
      if (values.get(i) instanceof Boolean) {
        assertTrue(vector.isNullAt(i));
      } else if (values.get(i) instanceof String) {
        try {
          final Decimal v = Utils.valueToDecimal(values.get(i), precision, scale);
          assertFalse(vector.isNullAt(i));
          assertEquals(v, vector.getDecimal(i, precision, scale));
          // System.out.println(v);
          // System.out.println(vector.getDecimal(i, precision, scale));
        } catch (final Exception e) {
          assertTrue(vector.isNullAt(i));
        }
      } else {
        try {
          final Decimal v = Utils.valueToDecimal(values.get(i), precision, scale);
          assertFalse(vector.isNullAt(i));
          assertEquals(v, vector.getDecimal(i, precision, scale));
          // System.out.println(v);
          // System.out.println(vector.getDecimal(i, precision, scale));
        } catch (final Exception e) {
          final int index = i;
          assertThrows(
              ArithmeticException.class,
              () -> {
                vector.getDecimal(index, precision, scale);
              });
        }
      }
    }
  }

  public static void assertIsNullAt(
      final WritableColumnVector vector, final int loadSize, final Set<Integer> keys) {
    for (int i = 0; i < loadSize; i++) {
      if (!keys.contains(i)) {
        assertTrue(vector.isNullAt(i));
      }
    }
  }

  @Test
  void T_isTargetColumnType() {
    // NOTE: test data
    // NOTE: expected
    final ColumnType[] columnTypes = {
        ColumnType.BYTE,
        ColumnType.BYTES,
        ColumnType.DOUBLE,
        ColumnType.FLOAT,
        ColumnType.INTEGER,
        ColumnType.LONG,
        ColumnType.SHORT,
        ColumnType.STRING
    };

    final int loadSize = 5;
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final SparkUnionDecimalLoader loader = new SparkUnionDecimalLoader(vector, loadSize);

    // NOTE: assert
    for (final ColumnType columnType : Utils.getColumnTypes()) {
      final boolean actual = loader.isTargetColumnType(columnType);
      if (Arrays.asList(columnTypes).contains(columnType)) {
        assertTrue(actual);
      } else {
        assertFalse(actual);
      }
    }
  }

  @Test
  void T_load_1() throws IOException {
    // NOTE: test data
    // NOTE: expected
    final int loadSize = 40;
    final Set<Integer> keys = new HashSet<>();
    final Map<Integer, Boolean> boValues =
        new HashMap<Integer, Boolean>() {
          {
            put(0, true);
            put(1, false);
          }
        };
    keys.addAll(boValues.keySet());
    final Map<Integer, Byte> byValues =
        new HashMap<Integer, Byte>() {
          {
            put(2, Byte.MIN_VALUE);
            put(3, Byte.MAX_VALUE);
          }
        };
    keys.addAll(byValues.keySet());
    final Map<Integer, String> btValues =
        new HashMap<Integer, String>() {
          {
            put(4, "ABCDEFGHIJ");
            put(5, "!\"#$%&'()*");
            put(6, String.valueOf(Byte.MIN_VALUE));
            put(7, String.valueOf(Byte.MAX_VALUE));
            put(8, String.valueOf(Long.MIN_VALUE));
            put(9, String.valueOf(Long.MAX_VALUE));
            put(10, String.valueOf(-1 * 123.123456789));
            put(11, String.valueOf(123.123456789));
          }
        };
    keys.addAll(btValues.keySet());
    final Map<Integer, Double> doValues =
        new HashMap<Integer, Double>() {
          {
            put(12, -1 * 123.123456789);
            put(13, 123.123456789);
            put(14, (double) Byte.MIN_VALUE);
            put(15, (double) Byte.MAX_VALUE);
          }
        };
    keys.addAll(doValues.keySet());
    final Map<Integer, Float> flValues =
        new HashMap<Integer, Float>() {
          {
            put(16, (float) -123.123456789);
            put(17, (float) 123.123456789);
            put(18, (float) Byte.MIN_VALUE);
            put(19, (float) Byte.MAX_VALUE);
          }
        };
    keys.addAll(flValues.keySet());
    final Map<Integer, Integer> inValues =
        new HashMap<Integer, Integer>() {
          {
            put(20, Integer.MIN_VALUE);
            put(21, Integer.MAX_VALUE);
            put(22, (int) Byte.MIN_VALUE);
            put(23, (int) Byte.MAX_VALUE);
          }
        };
    keys.addAll(inValues.keySet());
    final Map<Integer, Long> loValues =
        new HashMap<Integer, Long>() {
          {
            put(24, Long.MIN_VALUE);
            put(25, Long.MAX_VALUE);
            put(26, (long) Byte.MIN_VALUE);
            put(27, (long) Byte.MAX_VALUE);
          }
        };
    keys.addAll(loValues.keySet());
    final Map<Integer, Short> shValues =
        new HashMap<Integer, Short>() {
          {
            put(28, Short.MIN_VALUE);
            put(29, Short.MAX_VALUE);
            put(30, (short) Byte.MIN_VALUE);
            put(31, (short) Byte.MAX_VALUE);
          }
        };
    keys.addAll(shValues.keySet());
    final Map<Integer, String> stValues =
        new HashMap<Integer, String>() {
          {
            put(32, "ABCDEFGHIJ");
            put(33, "!\"#$%&'()*");
            put(34, String.valueOf(Byte.MIN_VALUE));
            put(35, String.valueOf(Byte.MAX_VALUE));
            put(36, String.valueOf(Long.MIN_VALUE));
            put(37, String.valueOf(Long.MAX_VALUE));
            put(38, String.valueOf(-1 * 123.123456789));
            put(39, String.valueOf(123.123456789));
          }
        };
    keys.addAll(stValues.keySet());

    // NOTE: create ColumnBinary
    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BOOLEAN, boValues);
    unionColumnUtils.add(ColumnType.BYTE, byValues);
    unionColumnUtils.add(ColumnType.BYTES, btValues);
    unionColumnUtils.add(ColumnType.DOUBLE, doValues);
    unionColumnUtils.add(ColumnType.FLOAT, flValues);
    unionColumnUtils.add(ColumnType.INTEGER, inValues);
    unionColumnUtils.add(ColumnType.LONG, loValues);
    unionColumnUtils.add(ColumnType.SHORT, shValues);
    unionColumnUtils.add(ColumnType.STRING, stValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(boValues, vector, loadSize, precision, scale);
    assertDecimal(byValues, vector, loadSize, precision, scale);
    assertDecimal(btValues, vector, loadSize, precision, scale);
    assertDecimal(doValues, vector, loadSize, precision, scale);
    assertDecimal(flValues, vector, loadSize, precision, scale);
    assertDecimal(inValues, vector, loadSize, precision, scale);
    assertDecimal(loValues, vector, loadSize, precision, scale);
    assertDecimal(shValues, vector, loadSize, precision, scale);
    assertDecimal(stValues, vector, loadSize, precision, scale);
    assertIsNullAt(vector, loadSize, keys);
  }

  @Test
  void T_load_2() throws IOException {
    // NOTE: test data
    // NOTE: expected: null,null,bo,by,bt,null,null,bo,null,by,null,bt,null,null
    final int loadSize = 14;
    final Set<Integer> keys = new HashSet<>();
    final Map<Integer, Boolean> boValues =
        new HashMap<Integer, Boolean>() {
          {
            put(2, true);
            put(7, false);
          }
        };
    keys.addAll(boValues.keySet());
    final Map<Integer, Byte> byValues =
        new HashMap<Integer, Byte>() {
          {
            put(3, Byte.MIN_VALUE);
            put(9, Byte.MAX_VALUE);
          }
        };
    keys.addAll(byValues.keySet());
    final Map<Integer, String> btValues =
        new HashMap<Integer, String>() {
          {
            put(4, "ABCDEFGHIJ");
            put(11, "!\"#$%&'()*");
          }
        };
    keys.addAll(btValues.keySet());

    // NOTE: create ColumnBinary
    final UnionColumnUtils unionColumnUtils = new UnionColumnUtils(loadSize);
    unionColumnUtils.add(ColumnType.BOOLEAN, boValues);
    unionColumnUtils.add(ColumnType.BYTE, byValues);
    unionColumnUtils.add(ColumnType.BYTES, btValues);
    final ColumnBinary columnBinary = unionColumnUtils.createColumnBinary();
    final IColumnBinaryMaker binaryMaker = unionColumnUtils.getBinaryMaker();

    // NOTE: load
    final int precision = DecimalType.MAX_PRECISION();
    final int scale = DecimalType.MINIMUM_ADJUSTED_SCALE();
    final DataType dataType = DataTypes.createDecimalType(precision, scale);
    final OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, dataType);
    final IUnionLoader<WritableColumnVector> loader = new SparkUnionDecimalLoader(vector, loadSize);
    binaryMaker.load(columnBinary, loader);

    // NOTE: assert
    assertDecimal(boValues, vector, loadSize, precision, scale);
    assertDecimal(byValues, vector, loadSize, precision, scale);
    assertDecimal(btValues, vector, loadSize, precision, scale);
    assertIsNullAt(vector, loadSize, keys);
  }
}