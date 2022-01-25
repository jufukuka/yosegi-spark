package jp.co.yahoo.yosegi.spark.inmemory.loader;

import org.apache.spark.sql.execution.vectorized.Dictionary;

import java.io.IOException;

public interface ISparkDictionary extends Dictionary {
  @Override
  default int decodeToInt(final int id) {
    throw new UnsupportedOperationException("decodeToInt is not supported.");
  }

  @Override
  default long decodeToLong(final int id) {
    throw new UnsupportedOperationException("decodeToLong is not supported.");
  }

  @Override
  default float decodeToFloat(final int id) {
    throw new UnsupportedOperationException("decodeToFloat is not supported.");
  }

  @Override
  default double decodeToDouble(final int id) {
    throw new UnsupportedOperationException("decodeToDouble is not supported.");
  }

  @Override
  default byte[] decodeToBinary(final int id) {
    throw new UnsupportedOperationException("decodeToBinary is not supported.");
  }

  default void setBytes(final int id, final byte[] value, final int start, final int length)
      throws IOException {
    throw new UnsupportedOperationException("setBytes is not supported.");
  }
}
