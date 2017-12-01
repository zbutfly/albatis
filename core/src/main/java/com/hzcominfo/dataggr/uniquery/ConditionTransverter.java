package com.hzcominfo.dataggr.uniquery;

import com.google.gson.JsonElement;
import com.google.gson.JsonPrimitive;

public interface ConditionTransverter {

    static Object valueOf(JsonElement element) {
        if (null == element || element.isJsonNull()) return null;
        if (element.isJsonPrimitive()) {
            JsonPrimitive primitive = element.getAsJsonPrimitive();
            if (primitive.isString()) return primitive.getAsString();
            if (primitive.isBoolean()) return primitive.getAsBoolean();
            if (primitive.isNumber()) return primitive.getAsNumber();
        }
        return element;
    }
}