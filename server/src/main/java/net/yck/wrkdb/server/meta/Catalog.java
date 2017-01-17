package net.yck.wrkdb.server.meta;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;

import avro.shaded.com.google.common.base.Objects;
import net.yck.wrkdb.common.util.ResourceUtil;

public class Catalog extends VersionedElement {
  private Collection<Schema> schemas;

  public Collection<Schema> getSchemas() {
    schemas = (schemas == null) ? new ArrayList<>() : schemas;
    return schemas;
  }

  public Catalog addSchema(Schema schema) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getSchemas(), schema, NamedElement.OptionOnDuplicate.THROW);
  }

  public Schema getSchema(String schema) {
    return getNamedElement(this, schemas, schema);
  }

  public Catalog updateSchema(Schema schema) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getSchemas(), schema, NamedElement.OptionOnDuplicate.UPDATE);
  }

  @Override
  public String toAvro() {
    return "[" + Joiner.on(", ").join(avroEntries()) + "]";
  }

  @Override
  protected List<String> avroEntries() {
    List<String> ret = new ArrayList<String>();
    for (Schema schema : getSchemas()) {
      ret.add(schema.toAvro());
    }
    return ret;
  }

  @Override
  protected NamedElement patchReferences() {
    return patchReferences(this, schemas);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj instanceof Catalog) {
      Catalog that = (Catalog) obj;
      boolean equal = super.equals(that);
      equal &= equals(this.schemas, that.schemas);
      return equal;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return schemas == null ? super.hashCode() : Objects.hashCode(super.hashCode(), schemas);
  }

  public static Catalog fromResource(Class<?> clz, String path) throws IOException {
    StringBuilder sb = new StringBuilder();
    try (BufferedReader br = ResourceUtil.getBufferedReader(clz, path)) {
      br.lines().forEach(l -> sb.append(l));
    }
    return NamedElement.fromJson(sb.toString(), Catalog.class);
  }

  public static Catalog fromJson(String json) throws IOException {
    return NamedElement.fromJson(json, Catalog.class);
  }
}
