package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryByteLoader extends AbstractSparkDictionaryNumberLoader {
  private byte[] dict;

  public SparkDictionaryByteLoader(final WritableColumnVector vector, final int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(final int index, final PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try {
        setByteToDic(index, value.getByte());
      } catch (final Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(final int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dict = new byte[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(final int index, final int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putByte(index, dict[dicIndex]);
    }
  }

  @Override
  public void setByteToDic(final int index, final byte value) throws IOException {
    dict[index] = value;
  }
}
