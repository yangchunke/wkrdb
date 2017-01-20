package net.yck.wrkdb.server.db;

import java.io.File;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.yck.wkrdb.common.util.ExecutorUtil;

public interface ITestSuite {

  final static Logger LOG = LogManager.getLogger(ITestSuite.class);
  final static String c_TmpDir = System.getProperty("java.io.tmpdir");
  final static Random rand = new Random(System.currentTimeMillis());
  final static int c_Iteration = 10;

  public static Path getPath(Class<?> clazz, String first, String... more)
      throws URISyntaxException {
    String relativePath = Paths.get(first, more).toString();
    if (!StringUtils.startsWith(relativePath, File.separator)) {
      relativePath = File.separator + relativePath;
    }
    URL resource = clazz.getResource(relativePath);
    return Paths.get(resource.toURI());
  }

  public static String getPathAsString(Class<?> clazz, String first, String... more)
      throws URISyntaxException {
    return getPath(clazz, first, more).toString();
  }
  
  public static boolean executeTasksAndWait(final Supplier<Runnable> taskSupplier,
      final String threadPoolName, final int nThreads, final long timeout, final TimeUnit timeUnit,
      final Logger logger) {
    ExecutorService taskExecutor = ExecutorUtil.newFixedThreadPool(nThreads, threadPoolName);
    for (int i = 0; i < nThreads; i++) {
      taskExecutor.execute(taskSupplier.get());
    }
    return ExecutorUtil.shutdown(taskExecutor, timeout, timeUnit, logger);
  }

}
