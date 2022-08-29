package org.functionstream.fsflow.controller;


import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.service.TokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RestController;
import static org.functionstream.fsflow.controller.TokenController.tokens;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@Slf4j
public class TokenController {

    public static TreeSet<String> tokens =new TreeSet<>();

    @Autowired
    private TokenService tokenService;

    private ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    @GetMapping("/user/login")
    public Map<String, Object> login(String username, String password) {
        try {
            tokenService.login(username, password);
            //create JWT
            String token = tokenService.generateToken(username);
            map.put("state", true);
            map.put("msg", "success");
            map.put("token", token);
        } catch (Exception e) {
            map.put("state", false);
            map.put("msg", e.getMessage());
        }
        return map;
    }

    @PostMapping("/user/check")
    public Map<String, Object> test() {
        map.put("msg", "success");
        map.put("state", true);
        return map;
    }

    @PostMapping("/user/logout")
    public Map<String, Object> logout(HttpServletRequest request) {
        String token = request.getHeader("token");
        tokens.remove("token0");
        map.put("msg", "success");
        map.put("state", true);
        return map;
    }
}
