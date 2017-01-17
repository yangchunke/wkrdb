package net.yck.wrkdb.server.meta;

import org.apache.commons.lang3.StringUtils;

import avro.shaded.com.google.common.base.Objects;

abstract class VersionedElement extends NamedElement {
    private String version;

    public String getVersion() {
        return version;
    }

    public VersionedElement setVersion(String version) {
        this.version = version;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VersionedElement) {
            VersionedElement that = (VersionedElement) obj;
            boolean equal = super.equals(that);
            equal &= StringUtils.endsWithIgnoreCase(this.version, that.version);
            return equal;
        }
        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(super.hashCode(), version);
    }
}
