package com.hzcominfo.dataggr.uniquery.test;

import com.google.gson.JsonObject;

public class GsonTest {
    public static void main(String[] args) {

        JsonObject json = new JsonObject();
        json.add("name", null);
        json.addProperty("age", 30);
        JsonObject j = new JsonObject();
        j.addProperty("xxx", 3333);
        j.addProperty("yyy", "vvvvv");
        json.add("obje", j);

        System.out.println(json.toString());

    }
}
