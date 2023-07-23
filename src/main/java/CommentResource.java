
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.auth0.jwk.JwkException;
import com.auth0.jwt.exceptions.JWTVerificationException;
import com.kumuluz.ee.discovery.annotations.DiscoverService;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.Map;
import java.util.*;
import java.util.logging.Logger;

@Path("/comments")
public class CommentResource {

    @Inject
    @DiscoverService(value = "product-service", environment = "dev", version = "1.0.0")
    private Optional<URL> productCatalogUrl;
    private static final Logger LOGGER = Logger.getLogger(CommentResource.class.getName());

    private DynamoDbClient dynamoDB = DynamoDbClient.builder()
            .region(Region.US_EAST_1)
            .build();
    private String tableName = "CommentDB";

    @GET
    @Path("/{productId}")
    public Response getProductComments(@PathParam("productId") String productId,
                                       @QueryParam("page") Integer page,
                                       @QueryParam("pageSize") Integer pageSize) {
        LOGGER.info("DynamoDB response: " + productCatalogUrl);
        if (page == null) {
            page = 1;
        }
        if (pageSize == null) {
            pageSize = 4;
        }

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":pid", AttributeValue.builder().s(productId).build());

        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(tableName)
                .keyConditionExpression("productId = :pid")
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        try {
            QueryResponse queryResponse = dynamoDB.query(queryRequest);

            int totalPages = (int) Math.ceil((double) queryResponse.items().size() / pageSize);

            int start = (page - 1) * pageSize;
            int end = Math.min(start + pageSize, queryResponse.items().size());
            List<Map<String, AttributeValue>> pagedItems = queryResponse.items().subList(start, end);

            List<Map<String, String>> itemsString = ResponseTransformer.transformItems(pagedItems);

            int totalComments = queryResponse.items().size();
            Map<String, Integer> ratingCounts = new HashMap<>();
            for (Map<String, AttributeValue> item : queryResponse.items()) {
                String rating = item.get("Rating").n();
                ratingCounts.put(rating, ratingCounts.getOrDefault(rating, 0) + 1);
            }

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("comments", itemsString);
            responseBody.put("totalPages", totalPages);
            responseBody.put("totalComments", totalComments);
            responseBody.put("ratingCounts", ratingCounts);

            return Response.ok(responseBody).build();

        } catch (DynamoDbException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }


    @POST
    @Path("/{productId}")
    @Consumes(MediaType.APPLICATION_JSON)
    public Response addCommentAndRating(@PathParam("productId") String productId,
                                        @HeaderParam("Auth") String token,
                                        CommentRating commentRating) {
        // Parse the token from the Authorization header
        LOGGER.info("DynamoDB response: " + token);
        LOGGER.info("DynamoDB response: " + commentRating);
        LOGGER.info("DynamoDB response: " + productId);
        String userId;
        // Verify the token and get the user's groups
        List<String> groups = null;
        try {
            userId = TokenVerifier.verifyToken(token, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_cl8iVMzUw");
            groups = TokenVerifier.getGroups(token, "https://cognito-idp.us-east-1.amazonaws.com/us-east-1_cl8iVMzUw");
        } catch (JWTVerificationException | JwkException | MalformedURLException e) {
            return Response.status(Response.Status.FORBIDDEN).entity("Invalid token.").build();
        }

        // Check if the user is in the "Admins" group
        if (groups == null || !groups.contains("Admins")) {
            return Response.status(Response.Status.FORBIDDEN).entity("Unauthorized: only admin users can add comments and ratings.").build();
        }

        try {
            Map<String, AttributeValue> item = new HashMap<>();
            item.put("UserId", AttributeValue.builder().s(userId).build());
            item.put("productId", AttributeValue.builder().s(productId).build());
            item.put("Comment", AttributeValue.builder().s(commentRating.getComment()).build());
            item.put("Rating", AttributeValue.builder().n(String.valueOf(commentRating.getRating())).build());

            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName(tableName)
                    .item(item)
                    .build();

            dynamoDB.putItem(putItemRequest);

            // After successfully adding the comment and rating, calculate the new average rating
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName(tableName)
                    .keyConditionExpression("productId = :v_id")
                    .expressionAttributeValues(Collections.singletonMap(":v_id", AttributeValue.builder().s(productId).build()))
                    .projectionExpression("Rating")
                    .build();
            QueryResponse queryResponse = dynamoDB.query(queryRequest);
            List<Map<String, AttributeValue>> comments = queryResponse.items();

            int totalRating = 0;
            for (Map<String, AttributeValue> commentItem : comments) {
                totalRating += Integer.parseInt(commentItem.get("Rating").n());
            }
            double avgRating = comments.size() > 0 ? Math.round((double) totalRating / comments.size()) : 0;
            LOGGER.info("DynamoDB response: " + avgRating);

           if (productCatalogUrl.isPresent()) {
               Client client = ClientBuilder.newClient();
               WebTarget target = client.target(productCatalogUrl.get().toString() + "/products/" + productId);
               Response response = target.request(MediaType.APPLICATION_JSON)
                       .header("Auth", token)
                       .put(Entity.entity(avgRating, MediaType.APPLICATION_JSON));

                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    return Response.status(response.getStatus()).entity(response.readEntity(String.class)).build();
                }
            }

            return Response.ok("Comment and rating added successfully").build();
        } catch (DynamoDbException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

}
