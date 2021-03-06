package net.yck.wrkdb.server;

import java.io.File;

import org.junit.BeforeClass;

import net.yck.wrkdb.server.db.ITestSuite;

public abstract class AppSelfService implements ITestSuite {
  static App app = null;

  @BeforeClass
  public static void beforeClass() throws Exception {
    if (app == null) {
      synchronized (AppSelfService.class) {
        if (app == null) {

          new File(DBManager.c_SvcDir).mkdirs();

          app = new App(new String[] {"-c", ITestSuite.getPathAsString(AppTestSuite.class, "dbserver.properties")});
          app.initialize();

          // // Start avro db server in a separate thread
          new Thread(app.avroDbServer, "avroDbServer").start();
          // // Start thrift server in a separate thread
          new Thread(app.thriftDbServer, "thriftDbServer").start();
          //
          // try {
          // // wait for the server start up
          // Thread.sleep(500);
          // }
          // catch (InterruptedException e) {
          // }
        }
      }
    }
  }

  protected static net.yck.wrkdb.service.thrift.DbService.Iface getThriftDbService() {
    return app.getThriftDbService();
  }

  protected static net.yck.wrkdb.service.avro.DbService getAvroDbService() {
    return app.getAvroDbService();
  }
}
