package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.StringObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;

import java.io.IOException;

public class SparkDictionaryBooleanLoader implements IDictionaryLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;
  private boolean[] dic;
  private boolean[] isNull;

  public SparkDictionaryBooleanLoader(final WritableColumnVector vector, final int loadSize) {
    this.vector = vector;
    this.loadSize = loadSize;
  }

  @Override
  public int getLoadSize() {
    return loadSize;
  }

  @Override
  public void setNull(int index) throws IOException {
    vector.putNull(index);
  }

  @Override
  public void finish() throws IOException {
    // FIXME:
  }

  @Override
  public WritableColumnVector build() throws IOException {
    return vector;
  }

  //  @Override
  //  public LoadType getLoaderType() {
  //    return IDictionaryLoader.super.getLoaderType();
  //  }

  @Override
  public void setBooleanToDic(int index, boolean value) throws IOException {
    dic[index] = value;
  }

  //  @Override
  //  public void setByteToDic(int index, byte value) throws IOException {
  //    IDictionaryLoader.super.setByteToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setShortToDic(int index, short value) throws IOException {
  //    IDictionaryLoader.super.setShortToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setIntegerToDic(int index, int value) throws IOException {
  //    IDictionaryLoader.super.setIntegerToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setLongToDic(int index, long value) throws IOException {
  //    IDictionaryLoader.super.setLongToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setFloatToDic(int index, float value) throws IOException {
  //    IDictionaryLoader.super.setFloatToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setDoubleToDic(int index, double value) throws IOException {
  //    IDictionaryLoader.super.setDoubleToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setBytesToDic(int index, byte[] value) throws IOException {
  //    IDictionaryLoader.super.setBytesToDic(index, value);
  //  }

  @Override
  public void setBytesToDic(int index, byte[] value, int start, int length) throws IOException {
    try {
      setBooleanToDic(index, new BytesObj(value, start, length).getBoolean());
    } catch (Exception e) {
      setNullToDic(index);
    }
  }

  @Override
  public void setStringToDic(int index, String value) throws IOException {
    try {
      setBooleanToDic(index, new StringObj(value).getBoolean());
    } catch (Exception e) {
      setNullToDic(index);
    }
  }

  //  @Override
  //  public void setStringToDic(int index, char[] value) throws IOException {
  //    IDictionaryLoader.super.setStringToDic(index, value);
  //  }
  //
  //  @Override
  //  public void setStringToDic(int index, char[] value, int start, int length) throws IOException
  // {
  //    IDictionaryLoader.super.setStringToDic(index, value, start, length);
  //  }

  @Override
  public void createDictionary(int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dic = new boolean[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(int index, int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putBoolean(index, dic[dicIndex]);
    }
  }

  @Override
  public void setNullToDic(int index) throws IOException {
    // FIXME: this method is not used in yosegi
    isNull[index] = true;
  }
}
