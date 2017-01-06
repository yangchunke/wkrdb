package net.yck.wrkdb.meta;

import java.util.ArrayList;
import java.util.Collection;

import avro.shaded.com.google.common.base.Objects;

public class UDT extends RecordElement<Attribute> {
    private Collection<Attribute> attributes;

    public Collection<Attribute> getAttributes() {
        attributes = (attributes == null) ? new ArrayList<>() : attributes;
        return attributes;
    }

    public UDT addAttribute(Attribute attribute) throws IllegalArgumentException {
        return addNamedElementToCollection(this, getAttributes(), attribute);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj instanceof UDT) {
            UDT that = (UDT) obj;
            boolean equal = super.equals(that);
            equal &= equals(this.attributes, that.attributes);
            return equal;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), attributes == null ? 0 : attributes);
    }

    @Override
    protected Collection<Attribute> getFields() {
        return getAttributes();
    }

    @Override
    protected NamedElement patchReferences() {
        return patchReferences(this, attributes);
    }
}
