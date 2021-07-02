import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import org.junit.Test;
import org.functionstream.functions.token.TokenJDBC;
import org.functionstream.functions.token.GetTokenFunction;

public class TokenTest {

    @Test
    public void testGetToken(){
        String s =null;
        GetTokenFunction tokenFunction = new GetTokenFunction();
        try {
            s = tokenFunction.process("{\"requestId\":\"1\",\"userName\":\"user-a\",\"password\":\"pwd\"}",null);
        } catch (Exception e) {
            e.printStackTrace();
        }
        System.out.println(s);
        if(s!=null&&s.length()!=0){
            JsonParser parser = new JsonParser();
            JsonElement element = parser.parse(s);
            JsonObject jsonObject = element.getAsJsonObject();
            String token = jsonObject.get("token").getAsString();
            System.out.println(GetTokenFunction.parseToken(token));
            System.out.println(GetTokenFunction.getUserNameByToken(token));
        }
    }
    @Test
    public void testCreateToken(){
        String token=GetTokenFunction.createToken("user-a");
        System.out.println(token);
        System.out.println(GetTokenFunction.parseToken(token));
        System.out.println(GetTokenFunction.getUserNameByToken(token));
    }

    @Test
    public void testCheckAccount(){
        System.out.println(TokenJDBC.checkAccount("abc", "123"));
    }
}
