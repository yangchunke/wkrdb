package net.yck.wkrdb.common.shared;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

public class PropertyBagTestSuite {

  private enum MyOwn {
    NA, FOO, BAR
  }

  @Test
  public void test() {
    PropertyBag pb = new PropertyBag();

    pb.set("int", 1);
    pb.set("float", 1.0f);
    pb.set("long", 2L);
    pb.set("str", "str");
    pb.set("enum", MyOwn.FOO);

    verify(pb);

    verify(new PropertyBag(pb));

    verify(new PropertyBag(pb.properties));
  }

  @Test
  public void testConverters() {
    PropertyBag pb = new PropertyBag();

    pb.set("int", 0, x -> Integer.toString(x + 1));
    pb.set("float", 0.0f, x -> Float.toString(x + 1.0f));
    pb.set("long", 0L, x -> Long.toString(x + 2L));
    pb.set("str", StringUtils.EMPTY, x -> x + "str");
    pb.set("enum", MyOwn.NA, x -> MyOwn.FOO.toString());

    verify(pb);

    Assert.assertTrue("int", 0 == pb.get("int", 1, x -> Integer.parseInt(x) - 1));
    Assert.assertTrue("float", 0.0f == pb.get("float", 1.0f, x -> Float.parseFloat(x) - 1.0f));
    Assert.assertTrue("long", 0L == pb.get("long", 2L, x -> Long.parseLong(x) - 2L));
    Assert.assertTrue("str",
        StringUtils.equals(StringUtils.EMPTY, pb.get("str", "str", x -> x.replaceAll("str", StringUtils.EMPTY))));
    Assert.assertTrue("enum", MyOwn.NA == pb.get("enum", MyOwn.FOO, x -> MyOwn.NA));
  }

  private void verify(PropertyBag pb) {
    Assert.assertTrue("int", 1 == pb.get("int", 0));
    Assert.assertTrue("float", 1.0f == pb.get("float", 0.0f));
    Assert.assertTrue("long", 2L == pb.get("long", 0L));
    Assert.assertTrue("str", StringUtils.equals("str", pb.get("str", StringUtils.EMPTY)));
    Assert.assertTrue("enum", MyOwn.FOO == pb.get("enum", MyOwn.NA));
  }

}
