package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import org.apache.spark.sql.execution.vectorized.OnHeapColumnVector;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.DataTypes;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.junit.jupiter.api.Assertions.*;

class SparkDictionaryBooleanLoaderTest {
  @Test
  void T_constructor() {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IDictionaryLoader<WritableColumnVector> loader = new SparkDictionaryBooleanLoader(vector, loadSize);
  }

  @Test
  void T_setBooleanToDic_1() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IDictionaryLoader<WritableColumnVector> loader = new SparkDictionaryBooleanLoader(vector, loadSize);
    loader.createDictionary(2);
    loader.setBooleanToDic(0, true);
    loader.setBooleanToDic(1, false);
    loader.setDictionaryIndex(0, 0);
    loader.setDictionaryIndex(1, 0);
    loader.setDictionaryIndex(2, 1);
    loader.setDictionaryIndex(3, 1);
    loader.setDictionaryIndex(4, 0);
    assertTrue(vector.getBoolean(0));
    assertTrue(vector.getBoolean(1));
    assertFalse(vector.getBoolean(2));
    assertFalse(vector.getBoolean(3));
    assertTrue(vector.getBoolean(4));
  }

  @Test
  void T_setBooleanToDic_2() throws IOException {
    int loadSize = 5;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IDictionaryLoader<WritableColumnVector> loader = new SparkDictionaryBooleanLoader(vector, loadSize);
    loader.createDictionary(1);
    loader.setBooleanToDic(0, true);
    loader.setNull(0);
    loader.setNull(1);
    loader.setNull(2);
    loader.setNull(3);
    loader.setDictionaryIndex(4, 0);
    assertTrue(vector.isNullAt(0));
    assertTrue(vector.isNullAt(1));
    assertTrue(vector.isNullAt(2));
    assertTrue(vector.isNullAt(3));
    assertTrue(vector.getBoolean(4));
  }

  @Test
  void T_setBytesToDic_1() throws IOException {
    int loadSize = 2;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IDictionaryLoader<WritableColumnVector> loader = new SparkDictionaryBooleanLoader(vector, loadSize);
    loader.createDictionary(2);
    loader.setBytesToDic(0, "true".getBytes(StandardCharsets.UTF_8), 0, 4);
    loader.setBytesToDic(1, "false".getBytes(StandardCharsets.UTF_8), 0, 5);
    loader.setDictionaryIndex(0, 0);
    loader.setDictionaryIndex(1, 1);
    assertTrue(vector.getBoolean(0));
    assertFalse(vector.getBoolean(1));
  }

  @Test
  void T_setStringToDic_1() throws IOException {
    int loadSize = 2;
    OnHeapColumnVector vector = new OnHeapColumnVector(loadSize, DataTypes.BooleanType);
    IDictionaryLoader<WritableColumnVector> loader = new SparkDictionaryBooleanLoader(vector, loadSize);
    loader.createDictionary(2);
    loader.setStringToDic(0, "true");
    loader.setStringToDic(1, "false");
    loader.setDictionaryIndex(0, 0);
    loader.setDictionaryIndex(1, 1);
    assertTrue(vector.getBoolean(0));
    assertFalse(vector.getBoolean(1));
  }
}