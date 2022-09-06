package org.functionstream.fsflow.controller;

import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@Slf4j
public class OtherController {
    private ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    @PostMapping("/**")
    public Map<String, Object> test() {
        map.put("msg", "success");
        map.put("state", true);
        return map;
    }

}
