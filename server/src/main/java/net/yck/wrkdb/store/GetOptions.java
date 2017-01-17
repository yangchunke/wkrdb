package net.yck.wrkdb.store;

import java.util.Map;

import net.yck.wrkdb.core.DBOptions;

public class GetOptions extends DBOptions {

  private static final long      serialVersionUID = -5293724487863218002L;

  public final static GetOptions c_default        = new GetOptions();

  public static GetOptions from(Map<String, String> prop) {
    return new GetOptions().populate(prop);
  }

}
