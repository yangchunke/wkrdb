package net.yck.wrkdb.server.util;

import com.google.common.hash.Hashing;

import net.yck.wkrdb.server.db.DBOptions;

public abstract class Sharder {

  public static enum Type {
    NoOp {
      @Override
      Sharder getSharder() {
        return noOpSharder;
      }
    },
    Md5 {
      @Override
      Sharder getSharder() {
        return md5Sharder;
      }
    },
    Murmur3 {
      @Override
      Sharder getSharder() {
        return murmur3Sharder;
      }
    };

    abstract Sharder getSharder();
  }

  public static String Prop_SharderType = "Sharder.Type";

  public abstract int shard(byte[] key, int numOfShards);

  public static Sharder getSharder(DBOptions options) {
    String str = options.getProperty(Prop_SharderType, Type.NoOp.name());
    return Type.valueOf(str).getSharder();
  }

  private static int mod(int val, int modulus) {
    return Math.abs(val) % modulus;
  }

  private static Sharder noOpSharder    = new Sharder() {

                                          @Override
                                          public int shard(byte[] key, int numOfShards) {
                                            return 0;
                                          }
                                        };

  private static Sharder md5Sharder     = new Sharder() {

                                          @Override
                                          public int shard(byte[] key, int numOfShards) {
                                            return mod(Hashing.md5().newHasher().putBytes(key).hash().asInt(),
                                                numOfShards);
                                          }
                                        };

  private static Sharder murmur3Sharder = new Sharder() {

                                          @Override
                                          public int shard(byte[] key, int numOfShards) {
                                            return mod(Hashing.murmur3_32().newHasher().putBytes(key).hash().asInt(),
                                                numOfShards);
                                          }
                                        };
}
