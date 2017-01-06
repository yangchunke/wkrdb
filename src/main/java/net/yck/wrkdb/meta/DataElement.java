package net.yck.wrkdb.meta;

import java.util.List;

import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;

import avro.shaded.com.google.common.base.Objects;

abstract class DataElement extends NamedElement {

    public static enum Type {
        STRING, BYTES, INT, LONG, FLOAT, DOUBLE, BOOLEAN, TIMESTAMP {
            @Override
            public org.apache.avro.Schema.Type toAvro() {
                return org.apache.avro.Schema.Type.LONG;
            }
        },
        UDT {
            @Override
            public org.apache.avro.Schema.Type toAvro() {
                return org.apache.avro.Schema.Type.RECORD;
            }
        };

        public org.apache.avro.Schema.Type toAvro() {
            return org.apache.avro.Schema.Type.valueOf(this.name());
        }
    }

    private DataElement.Type type;
    private boolean nullable;
    private String udt;

    public DataElement.Type getType() {
        return type;
    }

    public DataElement setType(Type type) {
        this.type = type;
        if (Type.UDT != type) {
            setUdt(StringUtils.EMPTY);
        }
        return this;
    }

    public String getUdt() {
        return udt;
    }

    public DataElement setUdt(String udt) {
        Preconditions.checkArgument(StringUtils.isEmpty(udt) || DataElement.Type.UDT == type);
        this.udt = udt;
        return this;
    }

    public boolean isNullable() {
        return nullable;
    }

    public DataElement setNullable(boolean nullable) {
        this.nullable = nullable;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (super.equals(obj)) {
            if (obj instanceof DataElement) {
                DataElement that = (DataElement) obj;
                return this.type == that.type && StringUtils.equals(this.udt, that.udt);
            }
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), type, udt);
    }

    @Override
    protected List<String> avroEntries() {
        List<String> ret = super.avroEntries();
        String avroTypeName = StringUtils.isNotEmpty(udt) ? getFullName(udt) : getType().toAvro().getName();
        if (isNullable()) {
            ret.add(String.format("\"type\": [\"%s\", \"null\"]", avroTypeName));
        }
        else {
            ret.add(String.format("\"type\": \"%s\"", avroTypeName));
        }
        return ret;
    }
    
    @Override
    protected NamedElement patchReferences() {
        return this;
    }

    private String getFullName(String name) {
        NamedElement parent = getParent();
        while (parent != null && !(parent instanceof Schema)) {
            parent = parent.getParent();
        }
        if (parent instanceof Schema) {
            UDT udt = ((Schema) parent).getUDT(name);
            return Joiner.on('.').join(udt.getNamespace(), name);
        }
        return name;
    }
}
