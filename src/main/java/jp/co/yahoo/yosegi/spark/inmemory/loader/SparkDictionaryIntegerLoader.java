package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryIntegerLoader extends AbstractSparkDictionaryNumberLoader {
  private int[] dict;

  public SparkDictionaryIntegerLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setIntegerToDic(index, value.getInt());
      } catch (Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dict = new int[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(int index, int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putInt(index, dict[dicIndex]);
    }
  }

  @Override
  public void setIntegerToDic(int index, int value) throws IOException {
    dict[index] = value;
  }
}
