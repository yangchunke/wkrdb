package net.yck.wrkdb.store;

import java.util.Map;

import net.yck.wrkdb.core.DBOptions;

public class PutOptions extends DBOptions {

  private static final long      serialVersionUID = 1050494679491314859L;

  public final static PutOptions c_default        = new PutOptions();

  public static PutOptions from(Map<String, String> prop) {
    return new PutOptions().populate(prop);
  }

}
