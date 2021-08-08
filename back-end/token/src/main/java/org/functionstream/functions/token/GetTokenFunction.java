package org.functionstream.functions.token;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.apache.pulsar.functions.api.Context;
import org.apache.pulsar.functions.api.Function;

public class GetTokenFunction implements Function<String, String> {

    private static String SECRET = "21232f297a57a5a743894a0e4a801fc3";
    private static SignatureAlgorithm SIGNATUREALGORITHM = SignatureAlgorithm.HS256;
    private static long SURVIVALTIME = 60 * 60 * 1000;

    @Override
    public String process(String input, Context context) throws Exception {
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(input);
        JsonObject jsonObject = element.getAsJsonObject();
        String userName = jsonObject.get("userName").getAsString();
        String password = jsonObject.get("password").getAsString();

        //Verify username and password from the database
        if (TokenJDBC.checkAccount(userName, password)) {
            jsonObject.remove("userName");
            jsonObject.remove("password");
            String token = createToken(userName);
            jsonObject.add("token", new Gson().toJsonTree(token));
        } else {
            throw new Exception("GetTokenFunction:incorrect username or password");
        }
        return jsonObject.toString();
    }

    private static String createToken(String userName) {
        Key signingKey = new SecretKeySpec(SECRET.getBytes(), SIGNATUREALGORITHM.getJcaName());
        Map<String, Object> claims = new HashMap<>();
        claims.put("role", userName);
        JwtBuilder builder = Jwts.builder().setClaims(claims)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis() + SURVIVALTIME))
                .signWith(SIGNATUREALGORITHM, signingKey);

        return builder.compact();
    }
}
