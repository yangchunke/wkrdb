package net.yck.wrkdb.server.store;

import net.yck.wrkdb.server.db.ITestSuite;

public abstract class StoreTestSuiteBase implements ITestSuite {
    final protected static int c_MaxKeyLength = Long.BYTES;
    final protected static int c_MaxValueLength = 1024;
}
