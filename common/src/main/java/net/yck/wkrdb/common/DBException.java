package net.yck.wkrdb.common;

public class DBException extends RuntimeException {

  /**
   * 
   */
  private static final long serialVersionUID = 168044513209548724L;

  public DBException(String msg) {
    super(msg);
  }

  public DBException(Exception inner) {
    super(inner);
  }

}
