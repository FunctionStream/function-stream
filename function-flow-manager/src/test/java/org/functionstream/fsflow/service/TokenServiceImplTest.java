package org.functionstream.fsflow.service;

import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.FunctionFlowManagerApplication;
import org.functionstream.fsflow.entity.UserEntity;
import org.junit.Assert;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.boot.test.context.SpringBootTest;

import javax.annotation.Resource;

import static org.functionstream.fsflow.controller.TokenController.tokens;
import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest(classes = FunctionFlowManagerApplication.class)
@Slf4j
class TokenServiceImplTest {

    @Resource
    private TokenService tokenService;

    @BeforeEach
    void setup() {
        tokenService.generateToken("123");
    }

    @Test
    void generateToken() {
        String token = tokenService.generateToken("sf");
        assertNotNull(token);
    }


    @Test
    void getToken() {
        String token = tokenService.generateToken("sf");
        Assertions.assertTrue(tokens.contains(token));
    }

    @Test
    void removeToken() {
        String token = tokenService.generateToken("sf");
        tokenService.removeToken(token);
        Assertions.assertFalse(tokens.contains(token));
    }


    @Test
    void login() {
        UserEntity user = tokenService.login("admin", "functionstream");
        assertNotNull(user);
    }
}