package org.functionstream.fsflow.service.impl;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.functionstream.fsflow.entity.UserEntity;
import org.functionstream.fsflow.service.TokenService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.Calendar;

import static org.functionstream.fsflow.controller.TokenController.tokens;

@Service

public class TokenServiceImpl implements TokenService {

    private static final String SING = "!@#Qq1";

    @Value("${function-stream.account}")
    private String account_p;

    @Value("${function-stream.password}")
    private String password_p;

    //生成
    @Override
    public String generateToken(String id) {
        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.SECOND, 600);

        JWTCreator.Builder builder = JWT.create();

        builder.withClaim("id", id);
        String token = builder.withExpiresAt(instance.getTime())
                .sign(Algorithm.HMAC256(SING));//sign

        tokens.add(token);

        return token;
    }

    //修改
    @Override
    public void setToken(String key, String value) {

        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.SECOND, 600);

        JWTCreator.Builder builder = JWT.create();

        builder.withClaim("id", key);
        String token = builder.withExpiresAt(instance.getTime())
                .sign(Algorithm.HMAC256(value));//sign
        tokens.add(token);
    }

    //查
    @Override
    public String getToken(String key) {
        if (tokens.contains(key)) {
            return key;
        } else {
            return null;
        }
    }

    //注销
    @Override
    public void removeToken(String key) {
        tokens.remove(key);
    }

    //验证
    public static DecodedJWT verify(String token) {
        return JWT.require(Algorithm.HMAC256(SING)).build().verify(token);
    }

    public UserEntity login(String username, String password) {
        if (!username.isEmpty() && !password.isEmpty()) {
            if (username.equals(account_p) && password.equals(password_p)) {
                return new UserEntity(username, password);
            }
        }
        throw new RuntimeException("fail login");
    }

}
