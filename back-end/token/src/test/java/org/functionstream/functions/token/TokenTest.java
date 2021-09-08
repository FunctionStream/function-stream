package org.functionstream.functions.token;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import io.jsonwebtoken.Claims;
import io.jsonwebtoken.Jwts;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TokenJdbcUtils.class)
@PowerMockIgnore("javax.crypto.*")
public class TokenTest {
    @InjectMocks
    private GetTokenFunction getTokenFunction;

    @Test
    public void testGetToken() throws Exception {
        String secret = "21232f297a57a5a743894a0e4a801fc3";
        String userName = "user-a";
        String password = "pwd";
        String input = "{\"requestId\":\"1\",\"userName\":\"user-a\",\"password\":\"pwd\"}";
        PowerMockito.mockStatic(TokenJdbcUtils.class);
        PowerMockito.when(TokenJdbcUtils.checkAccount(userName, password)).thenReturn(true);
        String actual = getTokenFunction.process(input, null);
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(actual);
        JsonObject jsonObject = element.getAsJsonObject();
        String token = jsonObject.get("token").getAsString();
        Claims body = Jwts.parser().setSigningKey(secret.getBytes()).parseClaimsJws(token).getBody();
        String role = body.get("role").toString();
        Assert.assertEquals("GetTokenFunction:token can't match", "user-a", role);
    }
}
