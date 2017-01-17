package net.yck.wrkdb.meta;

import java.io.IOException;
import java.util.Random;
import java.util.UUID;

import org.apache.commons.lang.StringUtils;
import org.junit.Assert;
import org.junit.Test;

import net.yck.wrkdb.common.util.JsonUtil;
import net.yck.wrkdb.meta.Catalog;
import net.yck.wrkdb.meta.NamedElement;

public class CatalogTestSuite extends MetaTestSuiteBase {

  public final static String c_SampleCatalogName = "tst";

  @Test
  public void testRandom() {
    for (int i = 0; i < c_Iteration; i++) {
      Catalog expected = i == 0 ? sampleCatalog() : newCatalog(rand);
      String json = expected.toString();
      Catalog actual = NamedElement.fromJson(json, Catalog.class);
      Assert.assertEquals(expected, actual);
    }
  }

  @Test
  public void testSample() {
    Catalog expected = sampleCatalog();
    String json = expected.toString();
    LOG.info(JsonUtil.prettify(json));
    LOG.info("=================================");
    LOG.info(JsonUtil.prettify(expected.toAvro()));
    Catalog actual = NamedElement.fromJson(json, Catalog.class);
    Assert.assertEquals(expected, actual);
  }

  @Test
  public void testDeserSample() {
    Catalog expected = sampleCatalog();
    try {
      Catalog actual = Catalog.fromResource(this.getClass(), "/sampleCatalog.json");
      Assert.assertEquals(expected, actual);
    } catch (IOException e) {
      Assert.fail(e.getMessage());
    }
  }

  public static Catalog newCatalog(Random rand) {
    Catalog catalog = (Catalog) new Catalog()//
        .setVersion(rand == null ? StringUtils.EMPTY : Integer.toString(rand.nextInt()))//
        .setName(UUID.randomUUID().toString());

    final int numOfSchema = rand == null ? 1 : rand.nextInt(c_MaxNumOfSchemas);
    for (int i = 0; i < numOfSchema; i++) {
      catalog.addSchema(SchemaTestSuite.newSchema(rand));
    }

    return catalog;
  }

  public static Catalog sampleCatalog() {
    return (Catalog) new Catalog()//
        .addSchema(SchemaTestSuite.sampleSchema())//
        .setVersion("1.0")//
        .setName(c_SampleCatalogName);
  }
}
