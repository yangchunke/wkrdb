package net.yck.wrkdb.store;

import net.yck.wrkdb.ITestSuite;

public abstract class StoreTestSuiteBase implements ITestSuite {
    final protected static int c_MaxKeyLength = Long.BYTES;
    final protected static int c_MaxValueLength = 1024;
}
