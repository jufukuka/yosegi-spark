package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.ISequentialLoader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SparkSequentialBooleanLoaderTest {
  @Test
  void T_constructor_1() {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    ISequentialLoader<WritableColumnVector> loader = new SparkSequentialBooleanLoader(vector, loadSize);
  }

  @Test
  void T_setBoolean_1() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    ISequentialLoader<WritableColumnVector> loader = new SparkSequentialBooleanLoader(vector, loadSize);
    loader.setBoolean(0, true);
    loader.setBoolean(1, false);
    loader.setBoolean(2, true);
    loader.setBoolean(3, false);
    loader.setBoolean(4, true);
    assertTrue(vector.getBoolean(0));
    assertFalse(vector.getBoolean(1));
    assertTrue(vector.getBoolean(2));
    assertFalse(vector.getBoolean(3));
    assertTrue(vector.getBoolean(4));
  }

  @Test
  void T_setBoolean_2() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    ISequentialLoader<WritableColumnVector> loader = new SparkSequentialBooleanLoader(vector, loadSize);
    loader.setNull(0);
    loader.setNull(1);
    loader.setNull(2);
    loader.setNull(3);
    loader.setBoolean(4, true);
    assertTrue(vector.isNullAt(0));
    assertTrue(vector.isNullAt(1));
    assertTrue(vector.isNullAt(2));
    assertTrue(vector.isNullAt(3));
    assertTrue(vector.getBoolean(4));
  }

  @Test
  void T_setBytes_1() throws IOException {
    int loadSize = 2;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    ISequentialLoader<WritableColumnVector> loader = new SparkSequentialBooleanLoader(vector, loadSize);
    loader.setBytes(0, "true".getBytes(StandardCharsets.UTF_8), 0, 4);
    loader.setBytes(1, "false".getBytes(StandardCharsets.UTF_8), 0, 5);
    assertTrue(vector.getBoolean(0));
    assertFalse(vector.getBoolean(1));
  }

  @Test
  void T_setString_1() throws IOException {
    int loadSize = 2;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    ISequentialLoader<WritableColumnVector> loader = new SparkSequentialBooleanLoader(vector, loadSize);
    loader.setString(0, "true");
    loader.setString(1, "false");
    assertTrue(vector.getBoolean(0));
    assertFalse(vector.getBoolean(1));
  }
}