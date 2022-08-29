package org.functionstream.fsflow.controller;

import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.functionstream.fsflow.FunctionFlowManagerApplication;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.request.MockMvcRequestBuilders;
import org.springframework.test.web.servlet.result.MockMvcResultHandlers;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

import javax.annotation.Resource;

import static org.functionstream.fsflow.controller.TokenController.tokens;

@Slf4j
@SpringBootTest(classes = {FunctionFlowManagerApplication.class}, webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureMockMvc
class UserControllerTest {

    @Autowired
    WebApplicationContext webApplicationContext;

    @Resource
    private MockMvc mockMvc;


    @BeforeEach

    public void setup() {
        mockMvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }

    @SneakyThrows
    @Test
    void login() {
        mockMvc.perform(MockMvcRequestBuilders
                        .get("/user/login")
                        .param("username", "admin")
                        .param("password", "functionstream"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.jsonPath("msg").value("success"))
                .andReturn();
        System.out.println(tokens);
    }

    @SneakyThrows
    @Test
    void logout() {
        mockMvc.perform(MockMvcRequestBuilders
                .get("/user/login")
                .param("username", "admin")
                .param("password", "functionstream"));

        mockMvc.perform(MockMvcRequestBuilders
                        .post("/user/logout"))
                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.jsonPath("msg").value("success"))
                .andReturn();
    }

    @SneakyThrows
    @Test
    void test1() {
        mockMvc.perform(MockMvcRequestBuilders
                .get("/user/login")
                .param("username", "admin")
                .param("password", "functionstream"));

        String s = tokens.pollFirst();
        tokens.add(s);
        mockMvc.perform(MockMvcRequestBuilders
                        .post("/user/check")
                        .header("token", s))

                .andExpect(MockMvcResultMatchers.status().isOk())
                .andDo(MockMvcResultHandlers.print())
                .andExpect(MockMvcResultMatchers.jsonPath("msg").value("success"))
                .andReturn();
    }
}