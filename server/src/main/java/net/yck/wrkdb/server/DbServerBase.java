package net.yck.wrkdb.server;

import java.io.IOException;
import java.util.Properties;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.logging.log4j.Logger;

import com.google.common.base.Preconditions;

import net.yck.wrkdb.common.util.ResourceUtil;

public abstract class DbServerBase extends ServerComponent implements Runnable {

  private final static Logger LOG = App.LOG;

  final DBManager             dbManager;

  private int                 port;
  private String              version;

  DbServerBase(App app) {
    super(app);
    Preconditions.checkArgument(app.dbManager != null);
    this.dbManager = app.dbManager;
  }

  @Override
  protected void doInitialize() throws Exception {
    port = app.config.getProperty(getPortProperty().left, getPortProperty().right);
  }

  /**
   * @return ImmutablePair<PropertyName, DefaultValue)
   */
  protected abstract ImmutablePair<String, Integer> getPortProperty();

  int getPort() {
    return port;
  }

  String getVersion() {
    if (StringUtils.isEmpty(version)) {
      synchronized (this) {
        if (StringUtils.isEmpty(version)) {
          try {
            Properties p = new Properties();
            p.load(ResourceUtil.getInputStream(this.getClass(), "/version.txt"));
            version = p.getProperty("version") + ".v" + p.getProperty("build.date");
          } catch (IOException e) {
            version = "N/A";
            LOG.error(() -> "failed to load version", e);
          }
        }
      }
    }
    return version;
  }

}
