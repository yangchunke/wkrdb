package net.yck.wrkdb.common.util;

public final class SysPropertyUtil {
  public static String getStringProperty(String name, String def) {
    String p = System.getProperty(name);
    return (p == null || p.isEmpty()) ? def : p;
  }

  public static int getIntProperty(String name, int def) {
    String p = System.getProperty(name);
    try {
      return (p == null || p.isEmpty()) ? def : Integer.parseInt(p);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  public static long getLongProperty(String name, long def) {
    String p = System.getProperty(name);
    try {
      return (p == null || p.isEmpty()) ? def : Long.parseLong(p);
    } catch (NumberFormatException e) {
      return def;
    }
  }

  public static boolean getBooleanProperty(String name, boolean def) {
    String p = System.getProperty(name);
    return (p == null || p.isEmpty()) ? def : Boolean.parseBoolean(p);
  }

  public static float getFloatProperty(String name, float def) {
    String p = System.getProperty(name);
    try {
      return (p == null || p.isEmpty()) ? def : Float.parseFloat(p);
    } catch (NumberFormatException e) {
      return def;
    }
  }
}
