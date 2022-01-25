package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IConstLoader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SparkConstBooleanLoaderTest {
  @Test
  void T_constructor_1() {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
  }

  @Test
  void T_setConstFromBoolean_1() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromBoolean(true);
    for (int i = 0; i < loadSize; i++) {
      assertTrue(vector.getBoolean(i));
    }
  }

  @Test
  void T_setConstFromBoolean_2() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromBoolean(false);
    for (int i = 0; i < loadSize; i++) {
      assertFalse(vector.getBoolean(i));
    }
  }

  @Test
  void T_setConstFromBytes_1() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromBytes("true".getBytes(StandardCharsets.UTF_8), 0, 4);
    for (int i = 0; i < loadSize; i++) {
      assertTrue(vector.getBoolean(i));
    }
  }

  @Test
  void T_setConstFromBytes_2() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromBytes("false".getBytes(StandardCharsets.UTF_8), 0, 5);
    for (int i = 0; i < loadSize; i++) {
      assertFalse(vector.getBoolean(i));
    }
  }

  @Test
  void T_setConstFromString_1() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromString("true");
    for (int i = 0; i < loadSize; i++) {
      assertTrue(vector.getBoolean(i));
    }
  }

  @Test
  void T_setConstFromString_2() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromString("false");
    for (int i = 0; i < loadSize; i++) {
      assertFalse(vector.getBoolean(i));
    }
  }

  @Test
  void T_setConstFromNull_1() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IConstLoader<WritableColumnVector> loader = new SparkConstBooleanLoader(vector, loadSize);
    loader.setConstFromNull();
    for (int i = 0; i < loadSize; i++) {
      assertTrue(vector.isNullAt(i));
    }
  }
}