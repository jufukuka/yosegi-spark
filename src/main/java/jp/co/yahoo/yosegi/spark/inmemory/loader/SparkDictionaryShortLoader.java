package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.message.objects.PrimitiveObject;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryShortLoader extends AbstractSparkDictionaryNumberLoader {
  private short[] dict;

  public SparkDictionaryShortLoader(WritableColumnVector vector, int loadSize) {
    super(vector, loadSize);
  }

  @Override
  public void setPrimitiveObjectToDic(int index, PrimitiveObject value) throws IOException {
    if (value == null) {
      setNullToDic(index);
    } else {
      try{
        setShortToDic(index, value.getShort());
      } catch (Exception e) {
        setNullToDic(index);
      }
    }
  }

  @Override
  public void createDictionary(int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dict = new short[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(int index, int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putShort(index, dict[dicIndex]);
    }
  }

  @Override
  public void setShortToDic(int index, short value) throws IOException {
    dict[index] = value;
  }
}
