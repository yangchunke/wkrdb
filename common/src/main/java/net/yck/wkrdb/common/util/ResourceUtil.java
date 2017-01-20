package net.yck.wkrdb.common.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.apache.commons.lang.StringUtils;

public final class ResourceUtil {

  public final static InputStream getInputStream(Class<?> clz, String file) throws IOException {
    if (!StringUtils.startsWith(file, File.separator)) {
      file = File.separator + file;
    }
    return FileIO.uncompressed(clz.getResourceAsStream(file), file);
  }

  public final static BufferedReader getBufferedReader(Class<?> clz, String file) throws IOException {
    return new BufferedReader(new InputStreamReader(getInputStream(clz, file)));
  }
}
