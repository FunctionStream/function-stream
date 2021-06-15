package org.functionstream.functions.token;

import com.google.gson.*;

import javax.crypto.spec.SecretKeySpec;

import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import io.jsonwebtoken.*;


public class Token {

    private static String secret="21232f297a57a5a743894a0e4a801fc3";

    private static SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;

    private static long SurvivalTime=60*60*1000;

    public static String changeToToken(String json) throws Exception {

        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(json);
        JsonObject jsonObject = element.getAsJsonObject();
        String userName = jsonObject.get("userName").getAsString();
        String password = jsonObject.get("password").getAsString();

        //Verify username and password from the database
        if(TokenJDBC.checkAccount(userName,password)){
            jsonObject.remove("userName");
            jsonObject.remove("password");
            String token = createToken(userName);
            jsonObject.add("token",new Gson().toJsonTree(token));
        }else {
            throw new Exception("incorrect username or password");
        }
        return jsonObject.toString();
    }

    public static String createToken(String userName){
        Key signingKey = new SecretKeySpec(secret.getBytes(), signatureAlgorithm.getJcaName());
        Map<String, Object> claims = new HashMap<>();
        claims.put("userName", userName);
        JwtBuilder builder = Jwts.builder().setClaims(claims)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis()+SurvivalTime))
                .signWith(signatureAlgorithm,signingKey);

        return builder.compact();
    }
    public static Claims parseToken(String token) {
        return Jwts.parser().setSigningKey(secret.getBytes())
                .parseClaimsJws(token).getBody();
    }
    public static String getUserNameByToken(String token){
        Claims body = Jwts.parser().setSigningKey(secret.getBytes()).parseClaimsJws(token).getBody();
        return body.get("userName").toString();
    }
}
