package org.functionstream.fsflow.controller;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.FunctionFlowManagerApplication;
import org.functionstream.fsflow.service.TokenService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.context.WebApplicationContext;

import javax.annotation.Resource;
import java.util.Map;


@Slf4j
@SpringBootTest(classes = {FunctionFlowManagerApplication.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class UserControllerTest {

    @Autowired
    WebApplicationContext webApplicationContext;

    @Autowired
    private TokenService tokenService;
    @Resource
    private MockMvc mockMvc;


    @BeforeEach

    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @SneakyThrows
    @Test
    void login() {
        String requestBody = "{username:admin,password:functionstream}";
        mockMvc.perform(MockMvcRequestBuilders
                        .post("/user/login")
                        .contentType(MediaType.APPLICATION_JSON).content(requestBody)
                        .accept(MediaType.APPLICATION_JSON))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.jsonPath("msg").value("success"))
                .andReturn();
    }

    @SneakyThrows
    @Test
    void logout() {
        String token = tokenService.generateToken("admin");

        mockMvc.perform(MockMvcRequestBuilders
                        .post("/user/logout")
                .header("token", token))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.jsonPath("msg").value("success"))
                .andReturn();
    }

    @SneakyThrows
    @Test
    void checkToken() {

        String token = tokenService.generateToken("admin");
        mockMvc.perform(MockMvcRequestBuilders
                        .post("/user/check")
                        .header("token", token))

                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.jsonPath("msg").value("success"))
                .andReturn();
    }

}