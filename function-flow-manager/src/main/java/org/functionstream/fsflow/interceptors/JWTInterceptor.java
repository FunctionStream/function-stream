package org.functionstream.fsflow.interceptors;

import com.auth0.jwt.exceptions.AlgorithmMismatchException;
import com.auth0.jwt.exceptions.TokenExpiredException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.functionstream.fsflow.service.TokenService;
import org.functionstream.fsflow.service.impl.JWTTokenServiceImpl;
import org.springframework.beans.factory.BeanFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Component;
import org.springframework.web.context.support.WebApplicationContextUtils;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;

@Component
public class JWTInterceptor implements HandlerInterceptor {

    @Autowired
    private TokenService tokenService;

    @Override
    public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler) throws Exception {
        if (tokenService == null) {
            BeanFactory factory = WebApplicationContextUtils
                    .getRequiredWebApplicationContext(request.getServletContext());
            tokenService = (TokenService) factory
                    .getBean("JWTTokenServiceImpl");
        }
        HashMap<String, Object> map = new HashMap<>();

        String token = request.getHeader("token");

        if (token.isEmpty() || !tokenService.getToken(token).equals(token)) {
            String json = new ObjectMapper()
                    .writeValueAsString(new ResponseEntity<>("token null or invalid", HttpStatus.BAD_REQUEST));
            response.getWriter().println(json);
            request.getRequestDispatcher("/user/login");
            return false;
        }
        try {
            JWTTokenServiceImpl.verify(token);
            map.put("msg", "success");
            map.put("state", true);
            return true;
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
        response.getWriter().println(json);
        request.getRequestDispatcher("/user/login");
        return false;
    }
}
