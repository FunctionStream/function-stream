package org.functionstream.functions.authentication;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.ExpiredJwtException;
import io.jsonwebtoken.Jwts;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class AuthenticationFunction implements Function<String,String> {

    private static String secret="21232f297a57a5a743894a0e4a801fc3";

    @Override
    public String process(String input, Context context) throws Exception {
        String role=null;
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(input);
        JsonObject jsonObject = element.getAsJsonObject();
        String token = jsonObject.get("token").getAsString();
        try{
            Claims body = Jwts.parser().setSigningKey(secret.getBytes()).parseClaimsJws(token).getBody();
            role = body.get("role").toString();
        }catch (ExpiredJwtException expiredJwtException){//jwt expired
            throw new Exception("AuthenticationFunction:jwt Expired");
        }catch (Exception e){
            throw new Exception("AuthenticationFunction:fail to verifySign");
        }
        //if the role exist and not null,the authentication is successful,otherwise illegal
        if(role!=null&&!"".equals(role)){
            jsonObject.remove("token");
            jsonObject.add("role",new Gson().toJsonTree(role));
            return jsonObject.toString();
        }else {
            throw new Exception("AuthenticationFunction:fail to verifySign");
        }
    }
}
