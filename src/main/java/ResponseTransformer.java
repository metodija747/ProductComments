import com.google.gson.*;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResponseTransformer {

    public static Map<String, String> transformItem(Map<String, AttributeValue> item) {
        Map<String, String> itemString = new HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            if (entry.getValue().s() != null) {
                itemString.put(entry.getKey(), entry.getValue().s());
            } else if (entry.getValue().n() != null) {
                itemString.put(entry.getKey(), entry.getValue().n());
            }
        }
        return itemString;
    }

    public static List<Map<String, String>> transformItems(List<Map<String, AttributeValue>> items) {
        List<Map<String, String>> itemsString = new ArrayList<>();
        for (Map<String, AttributeValue> item : items) {
            itemsString.add(transformItem(item));
        }
        return itemsString;
    }

    public static List<Map<String, String>> transformCartItems(List<Map<String, AttributeValue>> items) {
        List<Map<String, String>> transformedItems = new ArrayList<>();

        for (Map<String, AttributeValue> item : items) {
            Map<String, String> transformedItem = new HashMap<>();
            for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
                String key = entry.getKey();
                AttributeValue value = entry.getValue();

                if (key.equals("OrderList")) {
                    String orderListStr = value.s();
                    String[] orderList = orderListStr.split(";");
                    List<Map<String, String>> products = new ArrayList<>();
                    for (String order : orderList) {
                        String[] parts = order.split(":");
                        Map<String, String> product = new HashMap<>();
                        product.put("productId", parts[0]);
                        product.put("quantity", parts[1]);
                        products.add(product);
                    }
                    transformedItem.put("products", new Gson().toJson(products));
                } else if (key.equals("TotalPrice")) {
                    transformedItem.put(key, value.n());
                } else {
                    transformedItem.put(key, value.s());
                }
            }
            transformedItems.add(transformedItem);
        }

        return transformedItems;
    }

    public static Map<String, String> transformOrderItem(Map<String, AttributeValue> item) {
        Map<String, String> itemString = new HashMap<>();
        for (Map.Entry<String, AttributeValue> entry : item.entrySet()) {
            if (entry.getValue().s() != null) {
                // If the attribute is "OrderList", parse the JSON string into a list of objects
                if (entry.getKey().equals("OrderList")) {
                    String orderListStr = entry.getValue().s();
                    JsonParser parser = new JsonParser();
                    JsonArray orderList = parser.parse(orderListStr).getAsJsonArray();
                    List<Map<String, String>> products = new ArrayList<>();
                    for (JsonElement order : orderList) {
                        JsonObject orderObj = order.getAsJsonObject();
                        Map<String, String> product = new HashMap<>();
                        product.put("productName", orderObj.get("productName").getAsString());
                        product.put("quantity", orderObj.get("quantity").getAsString());
                        products.add(product);
                    }
                    Gson gson = new Gson();
                    itemString.put("OrderList", gson.toJson(products));
                } else {
                    itemString.put(entry.getKey(), entry.getValue().s());
                }
            } else if (entry.getValue().n() != null) {
                itemString.put(entry.getKey(), entry.getValue().n());
            }
        }
        return itemString;
    }


}
