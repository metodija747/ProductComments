import javax.enterprise.context.RequestScoped;
import javax.inject.Inject;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.kumuluz.ee.discovery.annotations.DiscoverService;
import com.kumuluz.ee.logs.cdi.Log;
import com.kumuluz.ee.logs.cdi.LogParams;
import io.opentracing.Span;
import io.opentracing.Tracer;
import org.eclipse.microprofile.faulttolerance.*;
import org.eclipse.microprofile.jwt.Claim;
import org.eclipse.microprofile.jwt.ClaimValue;
import org.eclipse.microprofile.jwt.Claims;
import org.eclipse.microprofile.jwt.JsonWebToken;
import org.eclipse.microprofile.metrics.Histogram;
import org.eclipse.microprofile.metrics.annotation.*;
import org.eclipse.microprofile.opentracing.Traced;
import org.eclipse.microprofile.rest.client.RestClientBuilder;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.*;

import java.net.MalformedURLException;
import java.net.URL;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.logging.Level;
import java.util.logging.Logger;

@Path("/comments")
@Log(LogParams.METRICS)
@Consumes(MediaType.APPLICATION_JSON)
@Produces(MediaType.APPLICATION_JSON)
@RequestScoped
public class CommentResource {

    @Inject
    private Tracer tracer;

    @Inject
    private ConfigProperties configProperties;

    @Inject
    @Claim("cognito:groups")
    private ClaimValue<Set<String>> groups;

    @Inject
    private JsonWebToken jwt;

    @Inject
    @Claim("sub")
    private ClaimValue<Optional<String>> optSubject;

    @Inject
    @DiscoverService(value = "catalog-service", environment = "dev", version = "1.0.0")
    private Optional<URL> productCatalogUrl;

    private static final Logger LOGGER = Logger.getLogger(CommentResource.class.getName());
    private DynamoDbClient dynamoDB;

    private volatile String currentRegion;
    private volatile String currentTableName;
    private void checkAndUpdateDynamoDbClient() {
        String newRegion = configProperties.getDynamoRegion();
        if (!newRegion.equals(currentRegion)) {
            try {
                this.dynamoDB = DynamoDbClient.builder()
                        .region(Region.of(newRegion))
                        .build();
                currentRegion = newRegion;
            } catch (Exception e) {
                LOGGER.severe("Error while creating DynamoDB client: " + e.getMessage());
                throw new WebApplicationException("Error while creating DynamoDB client: " + e.getMessage(), e, Response.Status.INTERNAL_SERVER_ERROR);
            }
        }
        currentTableName = configProperties.getTableName();
    }


    @GET
    @Path("/{productId}")
    @Retry(maxRetries = 3) // Retry up to 3 times
    @Fallback(fallbackMethod = "getProductCommentsFallback") // Fallback method if all retries fail
    @CircuitBreaker(requestVolumeThreshold = 4) // Use circuit breaker after 4 failed requests
    @Bulkhead(5) // Limit concurrent calls to 5
    @Counted(name = "getProductCommentsCount", description = "Count of getProductComments calls")
    @Timed(name = "getProductCommentsTime", description = "Time taken to fetch product comments")
    @Metered(name = "getProductCommentsMetered", description = "Rate of getProductComments calls")
    @Traced
    @Timeout(value = 20, unit = ChronoUnit.SECONDS) // Timeout after 20 seconds
    public Response getProductComments(@PathParam("productId") String productId,
                                       @QueryParam("page") Integer page,
                                       @QueryParam("pageSize") Integer pageSize) {
        if (page == null) {
            page = 1;
        }
        if (pageSize == null) {
            pageSize = 4;
        }
        Span span = tracer.buildSpan("getProductComments").start();
        span.setTag("productId", productId);
        Map<String, Object> logMap = new HashMap<>();
        logMap.put("event", "getProductComments");
        logMap.put("value", productId);
        span.log(logMap);
        checkAndUpdateDynamoDbClient();
        LOGGER.info("getProductComments method called");
        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":pid", AttributeValue.builder().s(productId).build());
        try {
        QueryRequest queryRequest = QueryRequest.builder()
                .tableName(currentTableName)
                .keyConditionExpression("productId = :pid")
                .expressionAttributeValues(expressionAttributeValues)
                .build();
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

            span.setTag("completed", true);
            return Response.ok(responseBody).build();

        } catch (DynamoDbException e) {
            LOGGER.log(Level.SEVERE, "Error while getting product comments " + productId, e);
            span.setTag("error", true);
            throw new WebApplicationException("Error while getting product comments. Please try again later.", e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            span.finish();
        }
    }
    public Response getProductCommentsFallback(@PathParam("productId") String productId,
                                               @QueryParam("page") Integer page,
                                               @QueryParam("pageSize") Integer pageSize) {
        LOGGER.info("Fallback activated: Unable to fetch product comments at the moment for productId: " + productId);
        Map<String, String> response = new HashMap<>();
        response.put("description", "Unable to fetch product comments at the moment. Please try again later.");
        return Response.ok(response).build();
    }


    @POST
    @Path("/{productId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Counted(name = "addCommentAndRatingCount", description = "Count of addCommentAndRating calls")
    @Timed(name = "addCommentAndRatingTime", description = "Time taken to add comment and rating")
    @Metered(name = "addCommentAndRatingMetered", description = "Rate of addCommentAndRating calls")
    @Timeout(value = 20, unit = ChronoUnit.SECONDS) // Timeout after 20 seconds
    @Retry(maxRetries = 3) // Retry up to 3 times
    @Fallback(fallbackMethod = "addCommentAndRatingFallback") // Fallback method if all retries fail
    @CircuitBreaker(requestVolumeThreshold = 4) // Use circuit breaker after 4 failed requests
    @Bulkhead(5) // Limit concurrent calls to 5
    @Traced
    public Response addCommentAndRating(@PathParam("productId") String productId,
                                        CommentRating commentRating) {
        Span span = tracer.buildSpan("addCommentAndRating").start();
        span.setTag("productId", productId);
        Map<String, Object> logMap = new HashMap<>();
        logMap.put("event", "addCommentAndRating");
        logMap.put("value", productId);
        logMap.put("comment", commentRating.getComment());
        logMap.put("rating", commentRating.getRating());
        span.log(logMap);
        checkAndUpdateDynamoDbClient();
        LOGGER.info("addCommentAndRating method called");
        if (jwt == null) {
            LOGGER.info("Unauthorized: only authenticated users can add/update rating to product.");
            return Response.ok("Unauthorized: only authenticated users can add/update rating to product.").build();
        }
        String userId = optSubject.getValue().orElse("default_value");
        try {
            Map<String, AttributeValue> attributeValues = new HashMap<>();
            attributeValues.put(":v_pid", AttributeValue.builder().s(productId).build());
            attributeValues.put(":v_uid", AttributeValue.builder().s(userId).build());

            QueryRequest userCommentCheckRequest = QueryRequest.builder()
                    .tableName(currentTableName)
                    .keyConditionExpression("productId = :v_pid and UserId = :v_uid")
                    .expressionAttributeValues(attributeValues)
                    .build();
            QueryResponse userCommentCheckResponse = dynamoDB.query(userCommentCheckRequest);

            Map<String, AttributeValue> item = new HashMap<>();
            item.put("UserId", AttributeValue.builder().s(userId).build());
            item.put("productId", AttributeValue.builder().s(productId).build());
            item.put("Comment", AttributeValue.builder().s(commentRating.getComment()).build());
            item.put("Rating", AttributeValue.builder().n(String.valueOf(commentRating.getRating())).build());

            PutItemRequest putItemRequest = PutItemRequest.builder()
                    .tableName(currentTableName)
                    .item(item)
                    .build();

            dynamoDB.putItem(putItemRequest);

            // After successfully adding the comment and rating, calculate the new average rating
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName(currentTableName)
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
            if (productCatalogUrl.isPresent()) {
                ProductCatalogApi api = RestClientBuilder.newBuilder()
                        .baseUrl(new URL(productCatalogUrl.get().toString()))
                        .build(ProductCatalogApi.class);

                String action = userCommentCheckResponse.items().isEmpty() ? "add" : "zero";
                String authHeader = "Bearer " + jwt.getRawToken(); // get the raw JWT token and prepend "Bearer "
                Response response = api.updateProductRating(productId, action, authHeader, avgRating);

                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    return Response.status(response.getStatus()).entity(response.readEntity(String.class)).build();
                }
            }
            span.setTag("completed", true);
            return Response.ok("Comment deleted successfully and average rating updated").build();
        } catch (DynamoDbException | MalformedURLException e) {
            LOGGER.log(Level.SEVERE, "Error while deleting comment and rating for product " + productId, e);
            span.setTag("error", true);
            throw new WebApplicationException("Error while deleting comment and rating. Please try again later.", e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            span.finish();
        }
    }
    public Response addCommentAndRatingFallback(@PathParam("productId") String productId,
                                                CommentRating commentRating) {
        LOGGER.info("Fallback activated: Unable to add comment and rating at the moment for productId: " + productId);
        Map<String, String> response = new HashMap<>();
        response.put("description", "Unable to add comment and rating at the moment. Please try again later.");
        return Response.ok(response).build();
    }

    @DELETE
    @Path("/{productId}")
    @Counted(name = "deleteCommentAndRatingCount", description = "Count of deleteCommentAndRating calls")
    @Timed(name = "deleteCommentAndRatingTime", description = "Time taken to delete comment and rating")
    @Metered(name = "deleteCommentAndRatingMetered", description = "Rate of deleteCommentAndRating calls")
    @Timeout(value = 20, unit = ChronoUnit.SECONDS) // Timeout after 20 seconds
    @Retry(maxRetries = 3) // Retry up to 3 times
    @Fallback(fallbackMethod = "deleteCommentAndRatingFallback") // Fallback method if all retries fail
    @CircuitBreaker(requestVolumeThreshold = 4) // Use circuit breaker after 4 failed requests
    @Bulkhead(5) // Limit concurrent calls to 5
    @Traced
    public Response deleteCommentAndRating(@PathParam("productId") String productId) {
        Span span = tracer.buildSpan("deleteCommentAndRating").start();
        span.setTag("productId", productId);
        Map<String, Object> logMap = new HashMap<>();
        logMap.put("event", "deleteCommentAndRating");
        logMap.put("value", productId);
        span.log(logMap);
        checkAndUpdateDynamoDbClient();
        LOGGER.info("deleteCommentAndRating method called");
        if (jwt == null) {
            LOGGER.info("Unauthorized: only authenticated users can add/update rating to product.");
            return Response.ok("Unauthorized: only authenticated users can add/update rating to product.").build();
        }
        String userId = optSubject.getValue().orElse("default_value");

        try {
            // Delete the comment
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("UserId", AttributeValue.builder().s(userId).build());
            key.put("productId", AttributeValue.builder().s(productId).build());

            DeleteItemRequest deleteItemRequest = DeleteItemRequest.builder()
                    .tableName(currentTableName)
                    .key(key)
                    .build();

            dynamoDB.deleteItem(deleteItemRequest);

            // After successfully deleting the comment, calculate the new average rating
            QueryRequest queryRequest = QueryRequest.builder()
                    .tableName(currentTableName)
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
            double avgRating = comments.size() > 0 ? (double) totalRating / comments.size() : 0;
            LOGGER.info("DynamoDB response: " + avgRating);

            if (productCatalogUrl.isPresent()) {
                ProductCatalogApi api = RestClientBuilder.newBuilder()
                        .baseUrl(new URL(productCatalogUrl.get().toString()))
                        .build(ProductCatalogApi.class);

                String action = "delete";
                String authHeader = "Bearer " + jwt.getRawToken(); // get the raw JWT token and prepend "Bearer "
                Response response = api.updateProductRating(productId, action, authHeader, avgRating);

                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    return Response.status(response.getStatus()).entity(response.readEntity(String.class)).build();
                }
            }

            return Response.ok("Comment deleted successfully and average rating updated").build();
        } catch (DynamoDbException | MalformedURLException e) {
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }
    public Response deleteCommentAndRatingFallback(@PathParam("productId") String productId) {
        LOGGER.info("Fallback activated: Unable to delete comment and rating at the moment for productId: " + productId);
        Map<String, String> response = new HashMap<>();
        response.put("description", "Unable to delete comment and rating at the moment. Please try again later.");
        return Response.ok(response).build();
    }


}
