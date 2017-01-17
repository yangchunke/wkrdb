package net.yck.wrkdb.server;

import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.thrift.server.TServer;
import org.apache.thrift.server.TThreadPoolServer;
import org.apache.thrift.server.TThreadPoolServer.Args;
import org.apache.thrift.transport.TServerSocket;
import org.apache.thrift.transport.TTransportException;

import net.yck.wrkdb.service.thrift.DbService.Processor;

public class ThriftDbServer extends DbServerBase {

  private final static Logger                         LOG                    =
      LogManager.getLogger(ThriftDbServer.class);

  private final static int                            DEF_MAX_WORKER_THREADS = 2048;

  private final static ImmutablePair<String, Integer> PROP_DEF_PAIR          =
      new ImmutablePair<String, Integer>(THRIFT_DBSERVER_PORT, 10719);

  private ThriftDbService                             iface;

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
      Processor<ThriftDbService> processor = new Processor<ThriftDbService>(getThriftIface());

      Args args = new Args(serverTransport)
          .maxWorkerThreads(app.config.getProperty(THRIFT_MAX_WORKER_THREADS, DEF_MAX_WORKER_THREADS))
          .processor(processor);

      TServer server = new TThreadPoolServer(args);
      LOG.info(() -> "Thrift DbService started successfully on port " + getPort());

      boolean keepRunning = true;
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

  ThriftDbService getThriftIface() {
    if (iface == null) {
      iface = new ThriftDbService(this, LOG);
    }
    return iface;
  }
}
