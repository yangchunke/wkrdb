package net.yck.wrkdb.server.store;

import java.util.Map;

import net.yck.wkrdb.server.db.DBOptions;

public class GetOptions extends DBOptions {

  private static final long      serialVersionUID = -5293724487863218002L;

  public final static GetOptions c_default        = new GetOptions();
  public final static GetOptions c_noCache        = c_default.clone();//.setUseCache(false);

  GetOptions() {}

  GetOptions(GetOptions other) {
    super(other);
  }

  public static GetOptions from(Map<String, String> prop) {
    return new GetOptions().populate(prop);
  }

  public GetOptions clone() {
    return new GetOptions(this);
  }

}
