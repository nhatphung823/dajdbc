package org.jdbc.mapper;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Locale;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 * User: Nguyen Van Nhat
 * Date: 11/16/12
 * Time: 3:37 PM
 * To change this template use File | Settings | File Templates.
 */
public class LinkedCaseInsensitiveMap<V> extends LinkedHashMap<String, V> {
  private final Map<String, String> caseInsensitiveKeys;
  private final Locale locale;

  public LinkedCaseInsensitiveMap() {
    this(null);
  }

  public LinkedCaseInsensitiveMap(Locale locale) {
    super();
    this.caseInsensitiveKeys = new HashMap<String, String>();
    this.locale = (locale != null ? locale : Locale.getDefault());
  }

  public LinkedCaseInsensitiveMap(int initialCapacity) {
    this(initialCapacity, null);
  }

  public LinkedCaseInsensitiveMap(int initialCapacity, Locale locale) {
    super(initialCapacity);
    this.caseInsensitiveKeys = new HashMap<String, String>(initialCapacity);
    this.locale = (locale != null ? locale : Locale.getDefault());
  }


  @Override
  public V put(String key, V value) {
    this.caseInsensitiveKeys.put(convertKey(key), key);
    return super.put(key, value);
  }

  @Override
  public void putAll(Map<? extends String, ? extends V> m) {
    for(Map.Entry<? extends String, ? extends V> e : m.entrySet()){
      put(e.getKey(), e.getValue());
    }
  }

  @Override
  public boolean containsKey(Object key) {
    return (key instanceof String && this.caseInsensitiveKeys.containsKey(convertKey((String) key)));
  }

  @Override
  public V get(Object key) {
    if (key instanceof String) {
      return super.get(this.caseInsensitiveKeys.get(convertKey((String) key)));
    } else {
      return null;
    }
  }

  @Override
  public V remove(Object key) {
    if (key instanceof String) {
      return super.remove(this.caseInsensitiveKeys.remove(convertKey((String) key)));
    } else {
      return null;
    }
  }

  @Override
  public void clear() {
    this.caseInsensitiveKeys.clear();
    super.clear();
  }

  protected String convertKey(String key) {
    return key.toLowerCase(this.locale);
  }
}
