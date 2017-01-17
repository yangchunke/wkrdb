package net.yck.wrkdb.server.meta;

import java.util.ArrayList;
import java.util.Collection;

import avro.shaded.com.google.common.base.Objects;
import avro.shaded.com.google.common.base.Preconditions;

public class Group extends RecordElement<Column> {

    private Collection<String> columns;

    public Collection<String> getColumns() {
        columns = (columns == null) ? new ArrayList<>() : columns;
        return columns;
    }

    public Group addColumn(Column column) throws IllegalArgumentException {
        return addStringToCollection(this, getColumns(), column.getName());
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof Group) {
            Group that = (Group) obj;
            boolean equal = super.equals(that);
            equal &= equals(this.columns, that.columns);
            return equal;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), columns == null ? 0 : columns);
    }

    @Override
    protected Collection<Column> getFields() {
        Preconditions.checkState(getParent() instanceof Table);
        Table table = (Table) getParent();
        Collection<Column> cols = new ArrayList<>();
        for (String col : getColumns()) {
            cols.add(table.getColumn(col));
        }
        return cols;
    }
}
