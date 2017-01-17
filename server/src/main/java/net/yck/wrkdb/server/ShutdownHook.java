package net.yck.wrkdb.server;

import org.apache.logging.log4j.Logger;

public class ShutdownHook extends ServerComponent implements Runnable {

  private final static Logger LOG = App.LOG;

  protected ShutdownHook(App app) {
    super(app);
  }

  @Override
  public void run() {
    LOG.info(() -> "shutting down...");
    try {
      ((App) app).dbManager.close();
    } catch (Exception e) {
      LOG.error(() -> e.getMessage());
    }
  }

  @Override
  protected void doInitialize() throws Exception {
    Runtime.getRuntime().addShutdownHook(new Thread(this, "shutdownHook"));
  }

}
