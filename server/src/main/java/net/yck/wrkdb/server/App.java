package net.yck.wrkdb.server;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

import net.yck.wrkdb.common.shared.AppBase;
import net.yck.wrkdb.service.thrift.DbService;

public class App extends AppBase {

    final static Logger LOG = LogManager.getLogger(App.class);

    DBManager dbManager;
    ThriftDbServer thriftDbServer;
    AvroDbServer avroDbServer;
    ShutdownHook shutdownHook;

    public static void main(String[] args) {
        App app = new App(args);
        try {
            app.initialize();
            app.start();
        }
        catch (Exception e) {
            LOG.fatal(() -> "failed to start the application.", e);
        }
    }

    App(String args[]) {
        super(args);
    }

    @SuppressWarnings("resource")
    void initialize() throws Exception {
        dbManager = (DBManager) new DBManager(this).initialize();

        // these three should be the last to be initialized
        thriftDbServer = (ThriftDbServer) new ThriftDbServer(this).initialize();
        avroDbServer = (AvroDbServer) new AvroDbServer(this).initialize();
        shutdownHook = (ShutdownHook) new ShutdownHook(this).initialize();
    }

    void start() {

        final List<Runnable> runnables = Arrays.asList(thriftDbServer, avroDbServer);

        final ListeningExecutorService pool = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(runnables.size()));

        final CountDownLatch cdl = new CountDownLatch(runnables.size());

        runnables.forEach(runnable -> {

            Futures.<Void> addCallback(pool.submit(runnable, null), new FutureCallback<Void>() {

                @Override
                public void onSuccess(Void result) {
                    cdl.countDown();
                }

                @Override
                public void onFailure(Throwable t) {
                    LOG.warn(() -> "execute", t);
                    cdl.countDown();
                }
            });
        });

        try {
            cdl.await();
        }
        catch (InterruptedException e) {
            LOG.error(() -> "execute", e);
        }
        finally {
            pool.shutdown();
        }
    }

    DbService.Iface getThriftIface() {
        return thriftDbServer.getThriftIface();
    }

    net.yck.wrkdb.service.avro.DbService getAvroDbService() {
        return avroDbServer.getDbService();
    }
}
