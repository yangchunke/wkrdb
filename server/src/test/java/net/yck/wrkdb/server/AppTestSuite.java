package net.yck.wrkdb.server;

import org.apache.avro.AvroRemoteException;
import org.apache.thrift.TException;
import org.junit.Assert;
import org.junit.Test;

public class AppTestSuite extends AppSelfService {

    @Test
    public void ping() {
        try {
            String v = getThriftDbService().ping();
            LOG.info(v);
            Assert.assertFalse(v.equals("N/A"));
        }
        catch (TException e) {
            Assert.fail(e.getMessage());
        }

        try {
            CharSequence v = getAvroDbService().ping();
            LOG.info(v);
            Assert.assertFalse(v.equals("N/A"));
        }
        catch (AvroRemoteException e) {
            Assert.fail(e.getMessage());
        }
    }
}
