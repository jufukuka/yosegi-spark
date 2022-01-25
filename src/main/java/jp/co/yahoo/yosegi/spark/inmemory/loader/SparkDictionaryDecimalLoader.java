package jp.co.yahoo.yosegi.spark.inmemory.loader;

import jp.co.yahoo.yosegi.inmemory.IDictionaryLoader;
import jp.co.yahoo.yosegi.message.objects.BytesObj;
import jp.co.yahoo.yosegi.message.objects.FloatObj;
import org.apache.spark.sql.execution.vectorized.WritableColumnVector;
import org.apache.spark.sql.types.Decimal;

import java.io.IOException;

public class SparkDictionaryDecimalLoader implements IDictionaryLoader<WritableColumnVector> {
  private final WritableColumnVector vector;
  private final int loadSize;

  private Decimal[] dic;
  private boolean[] isNull;

  public SparkDictionaryDecimalLoader(WritableColumnVector vector, int loadSize) {
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

  @Override
  public void setBooleanToDic(int index, boolean value) throws IOException {
    setNullToDic(index);
  }

  @Override
  public void setByteToDic(int index, byte value) throws IOException {
    setIntegerToDic(index, value);
  }

  @Override
  public void setShortToDic(int index, short value) throws IOException {
    setIntegerToDic(index, value);
  }

  @Override
  public void setIntegerToDic(int index, int value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    dic[index] = decimal;
  }

  @Override
  public void setLongToDic(int index, long value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    dic[index] = decimal;
  }

  @Override
  public void setFloatToDic(int index, float value) throws IOException {
    try {
      setDoubleToDic(index, new FloatObj(value).getDouble());
    } catch (Exception e) {
      setNullToDic(index);
    }
  }

  @Override
  public void setDoubleToDic(int index, double value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    dic[index] = decimal;
  }

//  @Override
//  public void setBytesToDic(int index, byte[] value) throws IOException {
//    IDictionaryLoader.super.setBytesToDic(index, value);
//  }

  @Override
  public void setBytesToDic(int index, byte[] value, int start, int length) throws IOException {
    try {
      setStringToDic(index, new BytesObj(value, start, length).getString());
    } catch (Exception e) {
      setNullToDic(index);
    }
  }

  @Override
  public void setStringToDic(int index, String value) throws IOException {
    Decimal decimal = Decimal.apply(value);
    vector.putDecimal(index, decimal, decimal.precision());
  }

//  @Override
//  public void setStringToDic(int index, char[] value) throws IOException {
//    IDictionaryLoader.super.setStringToDic(index, value);
//  }
//
//  @Override
//  public void setStringToDic(int index, char[] value, int start, int length) throws IOException {
//    IDictionaryLoader.super.setStringToDic(index, value, start, length);
//  }

  @Override
  public void createDictionary(int dictionarySize) throws IOException {
    isNull = new boolean[dictionarySize];
    dic = new Decimal[dictionarySize];
  }

  @Override
  public void setDictionaryIndex(int index, int dicIndex) throws IOException {
    if (isNull[dicIndex]) {
      setNull(index);
    } else {
      vector.putDecimal(index, dic[dicIndex], dic[dicIndex].precision());
    }
  }

  @Override
  public void setNullToDic(int index) throws IOException {
    isNull[index] = true;
  }
}
