import io.jsonwebtoken.JwtBuilder;
import io.jsonwebtoken.Jwts;
import io.jsonwebtoken.SignatureAlgorithm;
import org.functionstream.functions.authentication.AuthenticationFunction;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.modules.junit4.PowerMockRunner;

import javax.crypto.spec.SecretKeySpec;
import java.security.Key;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

@RunWith(PowerMockRunner.class)
@PowerMockIgnore("javax.crypto.*")
public class AuthenticationTest {
    private static String secret="21232f297a57a5a743894a0e4a801fc3";

    private static SignatureAlgorithm signatureAlgorithm = SignatureAlgorithm.HS256;

    private static long SurvivalTime=60*60*1000;

    @InjectMocks
    private AuthenticationFunction authenticationFunction;

    @Rule
    public ExpectedException thrown= ExpectedException.none();

    @Test
    public void authenticationTest() throws Exception {
        String input= "{\"requestId\":\"1\",\"token\":\"" +
                createToken("user-a",secret) +
                "\"}";
        String actual=authenticationFunction.process(input,null);
        Assert.assertEquals("AuthenticationFunction:not math","{\"requestId\":\"1\",\"role\":\"user-a\"}",actual);
    }
    @Test
    public void expiredTest() throws Exception {
        thrown.expect(Exception.class);
        thrown.expectMessage("AuthenticationFunction:jwt Expired");
        String ExpiredInput="{\"requestId\":\"1\",\"token\":\"eyJhbGciOiJIUzI1NiJ9.eyJ1c2VyTmFtZSI6IiIsImV4cCI6MTYyNTUwNDgxMSwiaWF0IjoxNjI1NTAxMjExfQ.inepBq8B_lWG3l3e3DLgCahahK0QNXOzYsP6gbFX2vo\"}";
        authenticationFunction.process(ExpiredInput,null);
    }
    @Test
    public void failToVerifySignTest() throws Exception {
        thrown.expect(Exception.class);
        thrown.expectMessage("AuthenticationFunction:fail to verifySign");
        String input= "{\"requestId\":\"1\",\"token\":\"" +
                createToken("user-b","014151") +
                "\"}";
        authenticationFunction.process(input,null);
    }

    private static String createToken(String userName,String secretInput){
        Key signingKey = new SecretKeySpec(secretInput.getBytes(), signatureAlgorithm.getJcaName());
        Map<String, Object> claims = new HashMap<>();
        claims.put("role", userName);
        JwtBuilder builder = Jwts.builder().setClaims(claims)
                .setIssuedAt(new Date())
                .setExpiration(new Date(System.currentTimeMillis()+SurvivalTime))
                .signWith(signatureAlgorithm,signingKey);

        return builder.compact();
    }
}
