package net.yck.wrkdb.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import com.google.common.base.Joiner;

import avro.shaded.com.google.common.base.Objects;

public class Table extends VersionedElement {
  private Collection<Column>         columns;
  private Collection<Group>          groups;
  private Key.Partition              partitionKey;
  private Key.Row                    rowKey;
  private Collection<Key.Clustering> clusteringKeys;

  public Collection<Column> getColumns() {
    columns = (columns == null) ? new ArrayList<>() : columns;
    return columns;
  }

  public Table addColumn(Column column) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getColumns(), column, NamedElement.OptionOnDuplicate.THROW);
  }

  public Collection<Group> getGroups() {
    groups = (groups == null) ? new ArrayList<>() : groups;
    return groups;
  }

  public Table addGroup(Group group) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getGroups(), group, NamedElement.OptionOnDuplicate.THROW);
  }

  public Key.Partition getPartitionKey() {
    return partitionKey;
  }

  public Table setPartitionKey(Key.Partition partitionKey) {
    this.partitionKey = (Key.Partition) partitionKey.setParent(this);
    return this;
  }

  public Key.Row getRowKey() {
    return rowKey;
  }

  public Table setRowKey(Key.Row rowKey) {
    this.rowKey = (Key.Row) rowKey.setParent(this);
    return this;
  }

  public Collection<Key.Clustering> getClusteringKeys() {
    clusteringKeys = (clusteringKeys == null) ? new ArrayList<>() : clusteringKeys;
    return clusteringKeys;
  }

  public Table addClusteringKey(Key.Clustering clusteringKey) throws IllegalArgumentException {
    return addNamedElementToCollection(this, getClusteringKeys(), (Key.Clustering) clusteringKey.setParent(this),
        NamedElement.OptionOnDuplicate.THROW);
  }

  @Override
  public String toAvro() {
    return Joiner.on(", ").join(avroEntries());
  }

  @Override
  protected List<String> avroEntries() {
    List<String> ret = new ArrayList<String>();
    for (Group group : getGroups()) {
      ret.add(group.toAvro());
    }
    ret.add(getPartitionKey().toAvro());
    if (getRowKey() != null) {
      ret.add(getRowKey().toAvro());
    }
    for (Key.Clustering key : getClusteringKeys()) {
      ret.add(key.toAvro());
    }
    return ret;
  }

  @SuppressWarnings("static-access")
  @Override
  protected NamedElement patchReferences() {
    return patchReferences(this, partitionKey)//
        .patchReferences(this, rowKey)//
        .patchReferences(this, columns)//
        .patchReferences(this, groups)//
        .patchReferences(this, clusteringKeys);
  }

  public Group getGroup(String name) throws IllegalArgumentException {
    return getNamedElement(this, groups, name);
  }

  Column getColumn(String name) {
    return getNamedElement(this, columns, name);
  }

  public Schema getSchema() {
    return (Schema) getParent();
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj instanceof Table) {
      Table that = (Table) obj;
      boolean equal = super.equals(that);
      equal &= equals(this.partitionKey, that.partitionKey);
      equal &= equals(this.rowKey, that.rowKey);
      equal &= equals(this.clusteringKeys, that.clusteringKeys);
      equal &= equals(this.columns, that.columns);
      equal &= equals(this.groups, that.groups);
      return equal;
    }
    return false;
  }

  @Override
  public int hashCode() {
    return Objects.hashCode(super.hashCode(), //
        partitionKey == null ? 0 : partitionKey, //
        rowKey == null ? 0 : rowKey, //
        clusteringKeys == null ? 0 : clusteringKeys, //
        columns == null ? 0 : columns, //
        groups == null ? 0 : groups);
  }
}
