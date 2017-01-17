package net.yck.wrkdb.common.util;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinPool.ForkJoinWorkerThreadFactory;
import java.util.concurrent.ForkJoinWorkerThread;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.ThreadFactoryBuilder;

public final class ExecutorUtil {

  /**
   * shutdown all the threads for the given thread pool
   * 
   * @param pool thread pool
   * @param timeout timeout value
   * @param timeUnit timeout unit
   * @param logger logger, can be null.
   * @return true if the thread pool was shut down in time
   */
  public final static boolean shutdown(final ExecutorService pool, final long timeout, final TimeUnit timeUnit,
      final Logger logger) {

    boolean ret = true;

    // Disable new tasks from being submitted
    pool.shutdown();
    try {

      // Wait a while for existing tasks to terminate
      if (!pool.awaitTermination(timeout, timeUnit)) {

        // Cancel currently executing tasks
        pool.shutdownNow();

        // Wait a while for tasks to respond to being cancelled
        if (!pool.awaitTermination(timeout, timeUnit)) {
          if (logger != null) {
            logger.error("Thread pool did not terminate in time (" + timeout + " " + timeUnit + ")");
          }

          ret = false;
        }
      }
    } catch (InterruptedException ie) {

      // (Re-)Cancel if current thread also interrupted
      pool.shutdownNow();

      // Preserve interrupt status
      Thread.currentThread().interrupt();
    }

    return ret;
  }

  /**
   * Creates a named work-stealing thread pool.
   * 
   * @param poolName
   * @return the newly created thread pool
   * @see ExecutorService#newWorkStealingPool()
   */
  public final static ExecutorService newWorkStealingPool(final String poolName) {
    return new ForkJoinPool(Runtime.getRuntime().availableProcessors(), new ForkJoinWorkerThreadFactory() {
      @Override
      public ForkJoinWorkerThread newThread(ForkJoinPool pool) {
        final ForkJoinWorkerThread worker = ForkJoinPool.defaultForkJoinWorkerThreadFactory.newThread(pool);
        worker.setName(poolName + "-" + worker.getPoolIndex());
        return worker;
      }
    }, null, true);
  }

  /**
   * Creates a named thread pool that reuses a fixed number of threads.
   * 
   * @param nThreads the number of threads in the pool
   * @param poolName the name of the pool
   * @return the newly created thread pool
   * @see Executors#newFixedThreadPool(int, ThreadFactory)
   */
  public final static ExecutorService newFixedThreadPool(int nThreads, final String poolName) {
    return Executors.newFixedThreadPool(nThreads, new ThreadFactoryBuilder().setNameFormat(poolName + "-%d").build());
  }
}
