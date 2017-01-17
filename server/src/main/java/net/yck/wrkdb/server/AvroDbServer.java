package net.yck.wrkdb.server;

import java.net.InetSocketAddress;

import org.apache.avro.ipc.NettyServer;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import net.yck.wrkdb.service.avro.DbService;

public class AvroDbServer extends DbServerBase {

  private final static Logger                         logger        = LogManager.getLogger(AvroDbServer.class);

  private final static ImmutablePair<String, Integer> PROP_DEF_PAIR =
      new ImmutablePair<String, Integer>(AVRO_DBSERVER_PORT, 10720);

  private Server                                      server;
  private DbService                                   svc;

  AvroDbServer(App app) {
    super(app);
  }

  @Override
  public void run() {
    try {
      server = new NettyServer(new SpecificResponder(DbService.class, getDbService()),
          new InetSocketAddress("localhost", getPort()));
      logger.info(() -> "Avro DbService started successfully on port " + getPort());
      server.join();
    } catch (Exception e) {
      logger.error(() -> "Failed to start Avro DbService", e);
    } finally {
      this.shutdown();
    }
  }

  @Override
  protected void doShutdown() {
    if (server != null) {
      server.close();
      server = null;
    }
  }

  @Override
  protected ImmutablePair<String, Integer> getPortProperty() {
    return PROP_DEF_PAIR;
  }

  DbService getDbService() {
    if (svc == null) {
      svc = new AvroDbService(this);
    }
    return svc;
  }
}
