package net.yck.wrkdb.meta;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;

import avro.shaded.com.google.common.base.Objects;

public class Schema extends VersionedElement {
  private Collection<Table> tables;
  private Collection<UDT>   udts;

  public Collection<Table> getTables() {
    tables = (tables == null) ? new ArrayList<>() : tables;
    return tables;
  }

  public Collection<UDT> getUDTs() {
    udts = (udts == null) ? new ArrayList<>() : udts;
    return udts;
  }

  public Schema addTable(Table table) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getTables(), table, NamedElement.OptionOnDuplicate.THROW);
  }

  public Schema addUDT(UDT udt) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getUDTs(), udt, NamedElement.OptionOnDuplicate.THROW);
  }

  public Table getTable(String table) {
    return getNamedElement(this, tables, table);
  }

  public UDT getUDT(String udt) {
    return getNamedElement(this, udts, udt);
  }

  @Override
  public String toAvro() {
    return "[" + Joiner.on(", ").join(avroEntries()) + "]";
  }

  @Override
  protected List<String> avroEntries() {
    List<String> ret = new ArrayList<String>();
    for (UDT udt : getUDTs()) {
      ret.add(udt.toAvro());
    }
    for (Table table : getTables()) {
      ret.add(table.toAvro());
    }
    return ret;
  }

  @SuppressWarnings("static-access")
  @Override
  protected NamedElement patchReferences() {
    return patchReferences(this, udts).patchReferences(this, tables);
  }

  public Catalog getCatalog() {
    return (Catalog) getParent();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj instanceof Schema) {
      Schema that = (Schema) obj;
      boolean equal = super.equals(that);
      equal &= equals(this.udts, that.udts);
      equal &= equals(this.tables, that.tables);
      return equal;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), udts == null ? 0 : udts, tables == null ? 0 : tables);
  }

  public static Schema fromJson(String json) throws IOException {
    return NamedElement.fromJson(json, Schema.class);
  }
}
