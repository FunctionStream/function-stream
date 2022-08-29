package org.functionstream.fsflow.interceptors;

import com.auth0.jwt.exceptions.AlgorithmMismatchException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.functionstream.fsflow.service.impl.TokenServiceImpl;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.concurrent.ConcurrentHashMap;
import static org.functionstream.fsflow.controller.TokenController.tokens;


public class JWTInterceptor implements HandlerInterceptor {

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {

        ConcurrentHashMap<String, Object> map = new ConcurrentHashMap<>();
        //get header

        String token = request.getHeader("token");
        if (token.isEmpty() || !tokens.contains(token)) {
            map.put("msg","token null");
            map.put("state", false);
            String json = new ObjectMapper().writeValueAsString(map);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().println(json);
            return false;
        }
        try {
                    TokenServiceImpl.verify(token); //check token
                return true; //success
            } catch (TokenExpiredException e) {
                e.printStackTrace();
                map.put("msg", "token over time");
            } catch (AlgorithmMismatchException e) {
                map.put("msg", "token Algorithm error");
                e.printStackTrace();
            } catch (Exception e) {
                map.put("msg", "token invalid");
                e.printStackTrace();
            }
            map.put("state", false);
            String json = new ObjectMapper().writeValueAsString(map);
            response.setContentType("application/json;charset=UTF-8");
            response.getWriter().println(json);
            return false;
    }
}
