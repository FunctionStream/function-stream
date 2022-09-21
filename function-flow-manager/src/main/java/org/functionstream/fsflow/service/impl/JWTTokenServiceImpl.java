package org.functionstream.fsflow.service.impl;

import com.auth0.jwt.JWT;
import com.auth0.jwt.JWTCreator;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.interfaces.DecodedJWT;
import org.functionstream.fsflow.service.TokenService;
import org.springframework.stereotype.Service;

import java.util.Calendar;
import java.util.TreeSet;

@Service
public class JWTTokenServiceImpl implements TokenService {

    private static final String SING = "!@#Qq1";

    public TreeSet<String> tokens = new TreeSet<>();

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

    @Override
    public void setToken(String key, String value) {

        Calendar instance = Calendar.getInstance();
        instance.add(Calendar.SECOND, 600);
        JWTCreator.Builder builder = JWT.create();

        builder.withClaim("id", key);
        String token = builder.withExpiresAt(instance.getTime())
                .sign(Algorithm.HMAC256(value));
        tokens.add(token);
    }

    @Override
    public String getToken(String key) {
        if (tokens.contains(key)) {
            return key;
        } else {
            return "";
        }
    }

    @Override
    public Boolean removeToken(String key) {
        tokens.remove(key);
        return true;
    }

    public static DecodedJWT verify(String token) {
        return JWT.require(Algorithm.HMAC256(SING)).build().verify(token);
    }


}
