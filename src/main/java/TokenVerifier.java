import com.auth0.jwk.Jwk;
import com.auth0.jwk.JwkException;
import com.auth0.jwk.JwkProvider;
import com.auth0.jwk.UrlJwkProvider;
import com.auth0.jwt.JWT;
import com.auth0.jwt.algorithms.Algorithm;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.auth0.jwt.interfaces.DecodedJWT;

import java.net.MalformedURLException;
import java.net.URL;
import java.security.interfaces.RSAPublicKey;
import java.util.List;

public class TokenVerifier {

    public static String verifyToken(String token, String issuer) throws JWTVerificationException, JwkException, MalformedURLException {
        DecodedJWT decodedJWT = JWT.decode(token);

        // Verify token signature
        JwkProvider provider = new UrlJwkProvider(new URL(issuer + "/.well-known/jwks.json"));
        Jwk jwk = provider.get(decodedJWT.getKeyId());
        Algorithm algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
        JWT.require(algorithm)
                .withIssuer(issuer)
                .build()
                .verify(token);

        return decodedJWT.getClaim("sub").asString();
    }

    public static List<String> getGroups(String token, String issuer) throws JWTVerificationException, JwkException, MalformedURLException {
        DecodedJWT decodedJWT = JWT.decode(token);

        // Verify token signature
        JwkProvider provider = new UrlJwkProvider(new URL(issuer + "/.well-known/jwks.json"));
        Jwk jwk = provider.get(decodedJWT.getKeyId());
        Algorithm algorithm = Algorithm.RSA256((RSAPublicKey) jwk.getPublicKey(), null);
        JWT.require(algorithm)
                .withIssuer(issuer)
                .build()
                .verify(token);

        return decodedJWT.getClaim("cognito:groups").asList(String.class);
    }
}
