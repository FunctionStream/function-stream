package org.functionstream.fsflow.controller;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;

import java.util.HashMap;

@Controller
public class TestController {
    @PostMapping("/**")
    public ResponseEntity test(){
        HashMap<String,Object> map =new HashMap<>();
        map.put("msg", "success");
        map.put("state", true);
        return new ResponseEntity<>(map, HttpStatus.OK);
    }
}
