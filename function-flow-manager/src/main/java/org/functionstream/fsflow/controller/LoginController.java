package org.functionstream.fsflow.controller;


import com.fasterxml.jackson.databind.util.TokenBuffer;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.entity.UserEntity;
import org.functionstream.fsflow.service.LoginService;
import org.functionstream.fsflow.service.TokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@RestController
@Slf4j
public class LoginController {

    @Autowired
    private TokenService tokenService;

    @Autowired
    private LoginService loginService;

    private ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();

    @PostMapping("/user/login")
    public Map<String, Object> login(@RequestBody UserEntity user) {
        String username = user.getUsername();
        String password = user.getPassword();

        try {
            loginService.login(username, password);
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


    @PostMapping("/user/logout")
    public Map<String, Object> logout(HttpServletRequest request) {
        String token = request.getHeader("token");
        tokenService.removeToken(token);
        map.put("msg", "success");
        map.put("state", true);
        return map;
    }
}
