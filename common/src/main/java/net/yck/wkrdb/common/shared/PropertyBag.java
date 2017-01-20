package net.yck.wkrdb.common.shared;

import java.util.HashMap;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Preconditions;

public final class PropertyBag {

  Map<String, String> properties = new HashMap<>();

  public PropertyBag() {}

  public PropertyBag(PropertyBag other) {
    this(other.properties);
  }

  public PropertyBag(Map<String, String> properties) {
    this.properties.putAll(properties);
  }

  public <T> T get(String property, T def, Function<String, T> converter) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(property));
    String raw = properties.get(property);
    return StringUtils.isEmpty(raw) ? def : converter.apply(raw);
  }

  @SuppressWarnings("unchecked")
  public <T> T get(String property, T def) {
    return get(property, def, x -> (T) PropertyConverter.to(def.getClass(), x));
  }

  public <T> void set(String property, T val, Function<T, String> converter) {
    Preconditions.checkArgument(StringUtils.isNotEmpty(property));
    Preconditions.checkNotNull(val);
    properties.put(property, converter.apply(val));
  }

  public <T> void set(String property, T val) {
    set(property, val, x -> x.toString());
  }

}
