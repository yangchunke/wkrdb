package net.yck.wrkdb.server;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.client.AvroDbClient;
import net.yck.wrkdb.client.ThriftDbClient;

public class AppTestSuite extends AppSelfService {

  @Test
  public void ping() {
    try (ThriftDbClient client = getThriftDbClient()) {
      String v = client.ping();
      LOG.info(v);
      Assert.assertFalse(StringUtils.equalsIgnoreCase("N/A", v));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }

    try (AvroDbClient client = AvroDbClient.getClient("localhost", app.avroDbServer.getPort())) {
      CharSequence v = client.ping();
      LOG.info(v);
      Assert.assertFalse(v.equals("N/A"));
    } catch (Exception e) {
      Assert.fail(e.getMessage());
    }
  }
}
