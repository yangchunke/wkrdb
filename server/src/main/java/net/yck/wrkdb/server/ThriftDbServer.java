package net.yck.wrkdb.server;

import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import net.yck.wkrdb.common.util.SysPropertyUtil;
import net.yck.wrkdb.service.thrift.DbService.Processor;

public class ThriftDbServer extends DbServerBase {

  private final static Logger                         LOG                       =
      LogManager.getLogger(ThriftDbServer.class);

  private static final int                            c_thrift_shutdown_maxwait =
      SysPropertyUtil.getIntProperty("wkrdb.thrift.shutdown.maxwait", 3);              // seconds
  private final static int                            DEF_MAX_WORKER_THREADS    = 1024;

  private final static ImmutablePair<String, Integer> PROP_DEF_PAIR             =
      new ImmutablePair<String, Integer>(THRIFT_DBSERVER_PORT, 10719);

  private TServer                                     server;
  private boolean                                     keepRunning               = true;
  private ThriftDbService                             service;

  ThriftDbServer(App app) {
    super(app);
  }

  @Override
  protected ImmutablePair<String, Integer> getPortProperty() {
    return PROP_DEF_PAIR;
  }

  @Override
  public void run() {
    try {
      TServerSocket serverTransport = new TServerSocket(this.getPort());
      Processor<ThriftDbService> processor = new Processor<ThriftDbService>(getDbService());

      Args args = new Args(serverTransport)
          .maxWorkerThreads(app.config.getProperty(THRIFT_MAX_WORKER_THREADS, DEF_MAX_WORKER_THREADS))
          .processor(processor);

      server = new TThreadPoolServer(args);
      LOG.info(() -> "Thrift DbService started successfully on port " + getPort());

      while (keepRunning) {
        try {
          server.serve();
        } catch (java.util.concurrent.RejectedExecutionException ree) {
          LOG.warn(() -> "Execution rejected", ree);
        } catch (Exception e) {
          LOG.fatal(() -> "Unexpected execution", e);
          keepRunning = false;
        }
      }
    } catch (TTransportException e) {
      LOG.fatal(() -> "Failed to start Thrift DbService", e);
    }
  }

  ThriftDbService getDbService() {
    if (service == null) {
      service = new ThriftDbService(this, LOG);
    }
    return service;
  }

  @Override
  protected void doShutdown() {
    if (server != null) {
      terminate();
      server = null;
    }
  }

  private void terminate() {
    try {
      keepRunning = false;
      server.stop();
      long start = System.currentTimeMillis();
      long maxwait = TimeUnit.SECONDS.toMillis(c_thrift_shutdown_maxwait);
      while (server.isServing() && (System.currentTimeMillis() - start < maxwait)) {
        Thread.sleep(100);
      }
      if (server.isServing()) {
        LOG.warn("unable to gracefully shut down thrift server.");
      }
    } catch (Exception ex) {
      LOG.error("Stopping thrift server exception", ex);
    }
  }
}
