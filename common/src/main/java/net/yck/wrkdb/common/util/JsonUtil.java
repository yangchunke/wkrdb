package net.yck.wrkdb.common.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonElement;
import com.google.gson.JsonParser;

public final class JsonUtil {
  public final static String prettify(String json) {
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    JsonElement je = new JsonParser().parse(json);
    return gson.toJson(je);
  }

}
