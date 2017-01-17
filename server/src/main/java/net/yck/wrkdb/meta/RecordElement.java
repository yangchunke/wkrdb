package net.yck.wrkdb.meta;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.List;

import com.google.common.base.Joiner;

abstract class RecordElement<C extends DataElement> extends DataElement {

    public String getNamespace() {
        Deque<String> segments = new ArrayDeque<String>();
        NamedElement parent = getParent();
        while (parent != null) {
            segments.push(parent.getName().toLowerCase());
            parent = parent.getParent();
        }
        return Joiner.on('.').join(segments);
    }

    public org.apache.avro.Schema getScopedAvroSchema(org.apache.avro.Schema schemaAvroSchema) {
        return schemaAvroSchema.getTypes().get(schemaAvroSchema.getIndexNamed(Joiner.on('.').join(this.getNamespace(), this.getName())));
    }

    @Override
    public DataElement.Type getType() {
        return DataElement.Type.UDT;
    }

    protected abstract Collection<C> getFields();

    @Override
    protected List<String> avroEntries() {

        List<String> ret = super.avroEntries();

        ret.add(String.format("\"namespace\": \"%s\"", getNamespace()));

        StringBuilder sb = new StringBuilder();
        sb.append("\"fields\" : [");
        List<String> fields = new ArrayList<>();
        for (C ne : getFields()) {
            fields.add(ne.toAvro());
        }
        sb.append(Joiner.on(", ").join(fields));
        sb.append("]");
        ret.add(sb.toString());

        return ret;
    }
}
