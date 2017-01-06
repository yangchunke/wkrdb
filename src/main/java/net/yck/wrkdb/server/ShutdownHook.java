package net.yck.wrkdb.server;

import org.apache.logging.log4j.Logger;

import net.yck.wrkdb.shared.AppBase;

public class ShutdownHook extends AppBase.Component implements Runnable {

    private final static Logger LOG = App.LOG;

    protected ShutdownHook(App app) {
        super(app);
    }

    @Override
    public void run() {
        LOG.info(() -> "shutting down...");
        try {
            ((App) app).dbManager.close();
        }
        catch (Exception e) {
            LOG.error(() -> e.getMessage());
        }
    }

    @Override
    public AppBase.Component initialize() throws Exception {
        Runtime.getRuntime().addShutdownHook(new Thread(this, "shutdownHook"));
        return this;
    }

}
