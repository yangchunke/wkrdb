package net.yck.wrkdb.server;

import java.io.File;
import java.net.SocketException;

import org.apache.thrift.transport.TTransportException;
import org.junit.BeforeClass;

import net.yck.wrkdb.ITestSuite;
import net.yck.wrkdb.client.ThriftDbClient;
import net.yck.wrkdb.server.App;
import net.yck.wrkdb.server.DBManager;

public abstract class AppSelfService implements ITestSuite {
    private final static Object lock = new Object();
    static App app = null;

    @BeforeClass
    public static void beforeClass() throws Exception {
        if (app == null) {
            synchronized (lock) {
                if (app == null) {

                    new File(DBManager.c_SvcDir).mkdirs();

                    app = new App(new String[] { "-c", ITestSuite.getPathAsString(AppTestSuite.class, "dbserver.properties") });
                    app.initialize();

                    // Start avro db server in a separate thread
                    new Thread(app.avroDbServer, "avroDbServer").start();
                    // Start thrift server in a separate thread
                    new Thread(app.thriftDbServer, "thriftDbServer").start();

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

    protected static ThriftDbClient getThriftDbClient() throws SocketException, TTransportException {
        return ThriftDbClient.getClient("localhost", app.thriftDbServer.getPort());
    }    
}
