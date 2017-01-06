package net.yck.wrkdb.meta;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Objects;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;

import com.google.common.base.Preconditions;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;

import avro.shaded.com.google.common.base.Joiner;

abstract class NamedElement implements Meta {
    private String name;
    private transient NamedElement parent;

    public String getName() {
        return name;
    }

    public NamedElement setName(String name) {
        this.name = name;
        return this;
    }

    public NamedElement getParent() {
        return parent;
    }

    public NamedElement setParent(NamedElement parent) {
        this.parent = parent;
        return this;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof NamedElement) {
            return StringUtils.equalsIgnoreCase(getName(), ((NamedElement) obj).getName());
        }
        return super.equals(obj);
    }

    @Override
    public int hashCode() {
        return Objects.hash(StringUtils.isEmpty(name) ? StringUtils.EMPTY : name.toUpperCase());
    }

    @Override
    public String toString() {
        return new GsonBuilder().setPrettyPrinting().create().toJson(this);
    }

    public String toAvro() {
        return "{" + Joiner.on(", ").join(avroEntries()) + "}";
    }

    @SuppressWarnings("serial")
    protected List<String> avroEntries() {
        return new ArrayList<String>() {
            {
                add(String.format("\"name\": \"%s\"", name));
            }
        };
    }

    protected abstract NamedElement patchReferences();

    @SuppressWarnings("unchecked")
    protected static <O extends NamedElement, T extends NamedElement> O addNamedElementToCollection(O owner, Collection<T> elements, T element) throws IllegalArgumentException {

        Preconditions.checkArgument(elements != null);
        Preconditions.checkArgument(element != null);
        Preconditions.checkArgument(StringUtils.isNoneBlank(element.getName()));

        for (T s : elements) {
            if (s.getName().equalsIgnoreCase(element.getName())) {
                throw new IllegalArgumentException("element '" + element.getName() + "' already exists.");
            }
        }

        elements.add((T) element.setParent(owner));

        return owner;
    }

    protected static <O extends NamedElement> O addStringToCollection(O owner, Collection<String> collections, String str) throws IllegalArgumentException {

        Preconditions.checkArgument(collections != null);
        Preconditions.checkArgument(StringUtils.isNoneBlank(str));

        for (String s : collections) {
            if (s.equalsIgnoreCase(str)) {
                throw new IllegalArgumentException("string '" + str + "' already exists.");
            }
        }

        collections.add(str);

        return owner;
    }

    protected static <O extends NamedElement, C extends NamedElement> C getNamedElement(O owner, Collection<C> elements, String name) {
        Preconditions.checkArgument(StringUtils.isNoneBlank(name));

        if (elements != null) {
            for (C s : elements) {
                if (s.getName().equalsIgnoreCase(name)) {
                    return s;
                }
            }
        }

        return null;
    }

    protected static <O extends NamedElement, C extends NamedElement> O patchReferences(O owner, C element) {
        if (element != null) {
            element.setParent(owner).patchReferences();
        }
        return owner;
    }

    protected static <O extends NamedElement, C extends NamedElement> O patchReferences(O owner, Collection<C> elements) {
        if (elements != null) {
            for (C s : elements) {
                s.setParent(owner).patchReferences();
            }
        }
        return owner;
    }

    protected static boolean equals(NamedElement obj1, NamedElement obj2) {
        if (obj1 == obj2) {
            return true;
        }
        else if (obj1 != null && obj2 != null) {
            return obj1.equals(obj2);
        }
        return false;
    }

    protected static boolean equals(Collection<?> col1, Collection<?> col2) {
        if (col1 == col2) {
            return true;
        }
        else if (col1 != null && col2 != null) {
            return CollectionUtils.isEqualCollection(col1, col2);
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    static <T extends NamedElement> T fromJson(String json, Class<T> clz) {
        return (T) new Gson().fromJson(json, clz).patchReferences();
    }
}
