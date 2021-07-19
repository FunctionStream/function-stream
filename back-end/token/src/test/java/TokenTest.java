import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Assert;
import org.junit.Test;
import org.functionstream.functions.token.TokenJDBC;
import org.functionstream.functions.token.GetTokenFunction;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(TokenJDBC.class)
@PowerMockIgnore("javax.crypto.*")
public class TokenTest {
    @InjectMocks
    private GetTokenFunction getTokenFunction;

    @Test
    public void testGetToken() throws Exception {
        String userName="user-a";
        String password="pwd";
        String input="{\"requestId\":\"1\",\"userName\":\"user-a\",\"password\":\"pwd\"}";
        PowerMockito.mockStatic(TokenJDBC.class);
        PowerMockito.when(TokenJDBC.checkAccount(userName,password)).thenReturn(true);
        String actual=getTokenFunction.process(input,null);
        JsonParser parser = new JsonParser();
        JsonElement element = parser.parse(actual);
        JsonObject jsonObject = element.getAsJsonObject();
        String token = jsonObject.get("token").getAsString();
        Assert.assertTrue("GetTokenFunction:token can't match",token.matches("\\w+[.]\\w+[.]\\w+"));
    }
}
