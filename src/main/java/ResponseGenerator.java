package lib;

import java.util.HashMap;
import java.util.Map;

public class ResponseGenerator {

    public static Map<String, Object> generateResponse(int statusCode, String body) {
        Map<String, Object> response = new HashMap<>();
        response.put("statusCode", statusCode);
        response.put("body", body);

        Map<String, String> headers = new HashMap<>();
        headers.put("Access-Control-Allow-Origin", "*"); // Allow from anywhere
        headers.put("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS"); // Allow these methods
        headers.put("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent"); // Allow these headers
        response.put("headers", headers);

        return response;
    }
}