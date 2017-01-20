package net.yck.wrkdb.server.store;

import java.util.Map;

import net.yck.wkrdb.server.db.DBOptions;

public class PutOptions extends DBOptions {

  private static final long      serialVersionUID = 1050494679491314859L;

  public final static PutOptions c_default        = new PutOptions();
  public final static PutOptions c_noCache        = c_default.clone();//.setUseCache(false);

  PutOptions() {}

  PutOptions(PutOptions other) {
    super(other);
  }

  public static PutOptions from(Map<String, String> prop) {
    return new PutOptions().populate(prop);
  }

  public PutOptions clone() {
    return new PutOptions(this);
  }

}
