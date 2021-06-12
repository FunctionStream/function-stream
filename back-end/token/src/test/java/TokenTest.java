import org.junit.Test;
import token.Token;

public class TokenTest {

    @Test
    public void testGetToken(){
        try {
            System.out.println(Token.changeToToken("{\"requestId\":\"1\",\"userName\":\"user-a\",\"password\":\"pwd\"}"));
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
    @Test
    public void testCreateToken(){
        String token=Token.createToken("user-a");
        System.out.println(token);
        System.out.println(Token.parseToken(token));
        System.out.println(Token.getUsernameByToken(token));
    }
}
