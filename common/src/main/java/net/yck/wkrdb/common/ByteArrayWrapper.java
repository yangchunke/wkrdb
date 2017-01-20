package net.yck.wkrdb.common;

import java.util.Arrays;

public final class ByteArrayWrapper {
  private final byte[] array;

  public ByteArrayWrapper(byte[] array) {
    if (array == null) {
      throw new NullPointerException();
    }
    this.array = array;
  }

  @Override
  public boolean equals(Object other) {
    if (!(other instanceof ByteArrayWrapper)) {
      return false;
    }
    return Arrays.equals(array, ((ByteArrayWrapper) other).array);
  }

  @Override
  public int hashCode() {
    return Arrays.hashCode(array);
  }

  public final byte[] array() {
    return array;
  }
}
