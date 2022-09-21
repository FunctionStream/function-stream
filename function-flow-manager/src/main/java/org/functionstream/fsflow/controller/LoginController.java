package org.functionstream.fsflow.controller;


import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.entity.UserEntity;
import org.functionstream.fsflow.service.LoginService;
import org.functionstream.fsflow.service.TokenService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;
import javax.servlet.http.HttpServletRequest;
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
    public ResponseEntity login(@RequestBody UserEntity user) {
        String username = user.getUsername();
        String password = user.getPassword();
        try {
            loginService.login(username, password);
            //create JWT
            map.put("msg", "success");
            map.put("state", true);
            map.put("token",tokenService.generateToken(username));
            return new ResponseEntity<>(map,HttpStatus.OK);
        } catch (Exception e) {
            map.put("msg", "failed");
            map.put("state", false);
            return new ResponseEntity<>(map,HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }


    @PostMapping("/user/logout")
    public ResponseEntity logout(HttpServletRequest request) {
        String token = request.getHeader("token");
        tokenService.removeToken(token);
        map.put("msg", "success");
        map.put("state", true);
        return new ResponseEntity<>(map,HttpStatus.OK);
    }

}
