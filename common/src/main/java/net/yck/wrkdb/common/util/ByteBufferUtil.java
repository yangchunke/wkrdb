package net.yck.wrkdb.common.util;

import java.nio.ByteBuffer;

public final class ByteBufferUtil {

  private ByteBufferUtil() {}

  public static ByteBuffer fromByteArray(byte[] array) {
    return array == null ? null : ByteBuffer.wrap(array);
  }

  public static byte[] toByteArray(ByteBuffer buffer) {
    return buffer != null && buffer.hasArray() ? buffer.array() : null;
  }

}
