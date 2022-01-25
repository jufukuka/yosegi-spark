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
  public void setPrimitiveObjectToDic(final int index, final PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setIntegerToDic(index, value.getInt());
      } catch (final Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dict = new int[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(final int index, final int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putInt(index, dict[dicIndex]);
    }
  }

  @Override
  public void setIntegerToDic(final int index, final int value) throws IOException {
    dict[index] = value;
  }
}
