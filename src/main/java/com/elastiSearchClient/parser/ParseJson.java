package com.elastiSearchClient.parser;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.stereotype.Component;
import org.springframework.stereotype.Controller;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

@Component
public class ParseJson {
    public Map<String,String> convertStringToMap(String jsonString){
        ObjectMapper mapper = new ObjectMapper();
        Map<String, String> map = new HashMap<>();
        try {
            // convert JSON string to Map
             map = mapper.readValue(jsonString, Map.class);
//            map.forEach((k, v) -> System.out.println((k + ":" + v)));
        } catch (IOException e) {
            e.printStackTrace();
        }
        return map;
    }
}
