package net.yck.wrkdb.server;

import java.io.File;

import org.junit.BeforeClass;

import net.yck.wrkdb.ITestSuite;

public abstract class AppSelfService implements ITestSuite {
    static App app = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (app == null) {
            synchronized (AppSelfService.class) {
                if (app == null) {

                    new File(DBManager.c_SvcDir).mkdirs();

                    app = new App(new String[] { "-c", ITestSuite.getPathAsString(AppTestSuite.class, "dbserver.properties") });
                    app.initialize();

                    // // Start avro db server in a separate thread
                    // new Thread(app.avroDbServer, "avroDbServer").start();
                    // // Start thrift server in a separate thread
                    // new Thread(app.thriftDbServer, "thriftDbServer").start();

                    try {
                        // wait for the server start up
                        Thread.sleep(500);
                    }
                    catch (InterruptedException e) {
                    }
                }
            }
        }
    }

    protected static net.yck.wrkdb.service.thrift.DbService.Iface getThriftIface() {
        return app.getThriftIface();
    }

    protected static net.yck.wrkdb.service.avro.DbService getAvroDbService() {
        return app.getAvroDbService();
    }

    // protected static ThriftDbClient getThriftDbClient() throws
    // SocketException, TTransportException {
    // return ThriftDbClient.getClient("localhost",
    // app.thriftDbServer.getPort());
    // }
}
