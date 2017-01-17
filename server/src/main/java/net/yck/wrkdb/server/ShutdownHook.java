package net.yck.wrkdb.server;

import org.apache.logging.log4j.Logger;

public class ShutdownHook extends ServerComponent {

  private final static Logger LOG = App.LOG;

  private Thread              hook;
  private boolean             shutdownComplete;

  protected ShutdownHook(App app) {
    super(app);
  }

  @Override
  protected void doShutdown() {
    LOG.info("***shutdown started***");
    ((App) app).shutdown();
    LOG.info("***shutdown completed***");
  }

  @Override
  protected void doInitialize() throws Exception {
    this.attach();
  }

  synchronized void detach() {
    if (hook != null) {
      Runtime.getRuntime().removeShutdownHook(hook);
      LOG.info("shutdown hook detached");
      hook = null;
    }
  }

  synchronized void attach() {
    if (hook == null) {

      hook = new Thread(new Runnable() {
        @Override
        public void run() {
          try {
            ShutdownHook.this.shutdown();
          } catch (Exception e) {
            LOG.info("shutdown", e);
          }
          shutdownComplete = true;
        }
      }, ShutdownHook.class.getSimpleName());

      Runtime.getRuntime().addShutdownHook(hook);
      LOG.info("shutdown hook attached");
    }
  }

  boolean shutdownInProcess() {
    return !shutdownComplete;
  }
}
