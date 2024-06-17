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
import org.eclipse.microprofile.openapi.annotations.Operation;
import org.eclipse.microprofile.openapi.annotations.media.Content;
import org.eclipse.microprofile.openapi.annotations.media.Schema;
import org.eclipse.microprofile.openapi.annotations.parameters.Parameter;
import org.eclipse.microprofile.openapi.annotations.parameters.RequestBody;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponse;
import org.eclipse.microprofile.openapi.annotations.responses.APIResponses;
import org.eclipse.microprofile.openapi.annotations.security.SecurityRequirement;
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
@SecurityRequirement(name = "jwtAuth")
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
    @Operation(summary = "Retrieve comments of a product by its ID",
            description = "Get paginated comments of a product along with associated rating counts.")
    @APIResponses({
            @APIResponse(responseCode = "200", description = "Comments fetched successfully",
                    content = @Content(mediaType = "application/json")),
            @APIResponse(responseCode = "500", description = "Internal server error")
    })
    @Path("/{productId}")
    @Retry(maxRetries = 3) // Retry up to 3 times
    @Fallback(fallbackMethod = "getProductCommentsFallback") // Fallback method if all retries fail
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 2000)
//    @Bulkhead(100) // Limit concurrent calls to 100
    @Counted(name = "getProductCommentsCount", description = "Count of getProductComments calls")
    @Timed(name = "getProductCommentsTime", description = "Time taken to fetch product comments")
    @Metered(name = "getProductCommentsMetered", description = "Rate of getProductComments calls")
    @Traced
    @Timeout(value = 50, unit = ChronoUnit.SECONDS) // Timeout after 50 seconds
    public Response getProductComments(
            @Parameter(description = "ID of the product to get comments for", required = true, example = "a9abe32e-9bd6-43aa-bc00-9044a27b858b")
            @PathParam("productId") String productId,

            @Parameter(description = "Page number for pagination", example = "1")
            @QueryParam("page") Integer page,

            @Parameter(description = "Number of comments per page", example = "4")
            @QueryParam("pageSize") Integer pageSize
    ) {
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
            LOGGER.info("Successfully obtained product comments.");
            return Response.ok(responseBody)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                    .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                    .build();
        } catch (DynamoDbException e) {
            LOGGER.log(Level.SEVERE, "Error while getting product comments for product " + productId, e);
            span.setTag("error", true);
            throw new RuntimeException("Failed to obtain product comments", e);
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
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                .entity(response)
                .build();
    }



    @POST
    @Operation(summary = "Add a comment and rating to a product",
            description = "Adds a comment and rating to a specific product identified by its productId.")
    @APIResponse(
            responseCode = "200",
            description = "Comment and rating added successfully.",
            content = @Content(schema = @Schema(implementation = CommentRating.class))
    )
    @APIResponse(
            responseCode = "401",
            description = "Unauthorized"
    )
    @APIResponse(
            responseCode = "500",
            description = "Internal Server Error"
    )
    @RequestBody(
            description = "Comment and rating object that needs to be added",
            required = true,
            content = @Content(
                    schema = @Schema(
                            implementation = CommentRating.class,
                            example = "{ \"comment\": \"Excellent product!\", \"rating\": 5, \"productId\": \"a9abe32e-9bd6-43aa-bc00-9044a27b858b\" }"
                    )
            )
    )
    @Consumes(MediaType.APPLICATION_JSON)
    @Counted(name = "addCommentAndRatingCount", description = "Count of addCommentAndRating calls")
    @Timed(name = "addCommentAndRatingTime", description = "Time taken to add comment and rating")
    @Metered(name = "addCommentAndRatingMetered", description = "Rate of addCommentAndRating calls")
    @Timeout(value = 50, unit = ChronoUnit.SECONDS) // Timeout after 20 seconds
    @Retry(maxRetries = 3) // Retry up to 3 times
    @Fallback(fallbackMethod = "addCommentAndRatingFallback") // Fallback method if all retries fail
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 2000)
    @Bulkhead(100) // Limit concurrent calls to 5
    @Traced
    public Response addCommentAndRating(CommentRating commentRating) {
        Span span = tracer.buildSpan("addCommentAndRating").start();
        span.setTag("productId", commentRating.getProductId());
        Map<String, Object> logMap = new HashMap<>();
        logMap.put("event", "addCommentAndRating");
        logMap.put("value", commentRating.getProductId());
        logMap.put("comment", commentRating.getComment());
        logMap.put("rating", commentRating.getRating());
        span.log(logMap);
        checkAndUpdateDynamoDbClient();
        LOGGER.info("addCommentAndRating method called");
        if (jwt == null) {
            LOGGER.log(Level.SEVERE, "Token verification failed");
            return Response.status(Response.Status.UNAUTHORIZED)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                    .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                    .entity("Invalid token.")
                    .build();
        }
        String userId = optSubject.getValue().orElse("default_value");
        try {
            Map<String, AttributeValue> attributeValues = new HashMap<>();
            attributeValues.put(":v_pid", AttributeValue.builder().s(commentRating.getProductId()).build());
            attributeValues.put(":v_uid", AttributeValue.builder().s(userId).build());

            QueryRequest userCommentCheckRequest = QueryRequest.builder()
                    .tableName(currentTableName)
                    .keyConditionExpression("productId = :v_pid and UserId = :v_uid")
                    .expressionAttributeValues(attributeValues)
                    .build();
            QueryResponse userCommentCheckResponse = dynamoDB.query(userCommentCheckRequest);


            Map<String, AttributeValue> item = new HashMap<>();
            item.put("UserId", AttributeValue.builder().s(userId).build());
            item.put("productId", AttributeValue.builder().s(commentRating.getProductId()).build());
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
                    .expressionAttributeValues(Collections.singletonMap(":v_id", AttributeValue.builder().s(commentRating.getProductId()).build()))
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
                Response responseFromCatalog = api.updateProductRating(commentRating.getProductId(), action, authHeader, avgRating);

                if (responseFromCatalog.getStatus() != Response.Status.OK.getStatusCode()) {
                    return Response.status(responseFromCatalog.getStatus())
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                            .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                            .entity(responseFromCatalog.readEntity(String.class))
                            .build();
                }
            }
            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("message", "Comment and rating added successfully");
            responseBody.put("averageRating", avgRating);

            LOGGER.info("Comment and rating added successfully");
            return Response.ok(responseBody)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                    .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                    .build();
        } catch (DynamoDbException | MalformedURLException e) {
            LOGGER.log(Level.SEVERE, "Error while adding comment and rating for product " + commentRating.getProductId(), e);
            span.setTag("error", true);
            throw new WebApplicationException("Error while adding comment and rating. Please try again later.", e, Response.Status.INTERNAL_SERVER_ERROR);
        } finally {
            span.finish();
        }
    }
    public Response addCommentAndRatingFallback(CommentRating commentRating) {
        LOGGER.info("Fallback activated: Unable to add comment and rating at the moment for productId: " + commentRating.getProductId());
        Map<String, String> response = new HashMap<>();
        response.put("description", "Unable to add comment and rating at the moment. Please try again later.");
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                .entity(response)
                .build();
    }

    @DELETE
    @Operation(
            summary = "Delete a comment and rating by product ID",
            description = "This operation deletes a comment and rating for a given product ID and recalculates the average rating for that product."
    )
    @APIResponses({
            @APIResponse(
                    responseCode = "200",
                    description = "Comment and rating deleted successfully"
            ),
            @APIResponse(
                    responseCode = "401",
                    description = "Unauthorized, invalid token"
            ),
            @APIResponse(
                    responseCode = "404",
                    description = "Comment not found"
            ),
            @APIResponse(
                    responseCode = "500",
                    description = "Internal server error"
            )
    })
    @Path("/{productId}")
    @Counted(name = "deleteCommentAndRatingCount", description = "Count of deleteCommentAndRating calls")
    @Timed(name = "deleteCommentAndRatingTime", description = "Time taken to delete comment and rating")
    @Metered(name = "deleteCommentAndRatingMetered", description = "Rate of deleteCommentAndRating calls")
    @Timeout(value = 50, unit = ChronoUnit.SECONDS) // Timeout after 20 seconds
    @Retry(maxRetries = 3) // Retry up to 3 times
    @Fallback(fallbackMethod = "deleteCommentAndRatingFallback") // Fallback method if all retries fail
    @CircuitBreaker(requestVolumeThreshold = 4, failureRatio = 0.5, delay = 2000)
    @Bulkhead(100) // Limit concurrent calls to 5
    @Traced
    public Response deleteCommentAndRating(@Parameter(description = "ID of the product to add comment and rating for", required = true, example = "a9abe32e-9bd6-43aa-bc00-9044a27b858b")
                                               @PathParam("productId") String productId) {
        Span span = tracer.buildSpan("deleteCommentAndRating").start();
        span.setTag("productId", productId);
        Map<String, Object> logMap = new HashMap<>();
        logMap.put("event", "deleteCommentAndRating");
        logMap.put("value", productId);
        span.log(logMap);
        checkAndUpdateDynamoDbClient();
        LOGGER.info("deleteCommentAndRating method called");
        if (jwt == null) {
            LOGGER.log(Level.SEVERE, "Token verification failed");
            return Response.status(Response.Status.UNAUTHORIZED)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                    .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                    .entity("Invalid token.")
                    .build();
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
            // Check if a comment with this UserId and productId exists
            if (userCommentCheckResponse.items() == null || userCommentCheckResponse.items().isEmpty()) {
                LOGGER.info("Comment with given userId and productId does not exist in the database.");
                Map<String, String> response = new HashMap<>();
                response.put("description", "Comment cannot be deleted because it is not present in the database.");
                return Response.status(Response.Status.NOT_FOUND)
                        .header("Access-Control-Allow-Origin", "*")
                        .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                        .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                        .entity(response)
                        .build();
            }
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

                String action = "delete"; // Assuming the action is 'delete' when deleting a comment
                String authHeader = "Bearer " + jwt.getRawToken(); // get the raw JWT token and prepend "Bearer "
                Response response = api.updateProductRating(productId, action, authHeader, avgRating);

                if (response.getStatus() != Response.Status.OK.getStatusCode()) {
                    return Response.status(response.getStatus())
                            .header("Access-Control-Allow-Origin", "*")
                            .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                            .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                            .entity(response.readEntity(String.class))
                            .build();
                }
            }

            Map<String, Object> responseBody = new HashMap<>();
            responseBody.put("message", "Comment and rating deleted successfully");
            responseBody.put("averageRating", avgRating);

            LOGGER.info("Comment and rating deleted successfully with new average rating: " + avgRating);
            return Response.ok(responseBody)
                    .header("Access-Control-Allow-Origin", "*")
                    .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                    .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                    .build();
        } catch (DynamoDbException | MalformedURLException e) {
            LOGGER.log(Level.SEVERE, "Error while deleting comment and rating for product " + productId, e);
            Map<String, String> response = new HashMap<>();
            throw new WebApplicationException("Error while deleting comment and rating. Please try again later.", e, Response.Status.INTERNAL_SERVER_ERROR);
        }
    }
    public Response deleteCommentAndRatingFallback(@PathParam("productId") String productId) {
        LOGGER.info("Fallback activated: Unable to delete comment and rating at the moment for productId: " + productId);
        Map<String, String> response = new HashMap<>();
        response.put("description", "Unable to delete comment and rating at the moment. Please try again later.");
        return Response.status(Response.Status.INTERNAL_SERVER_ERROR)
                .header("Access-Control-Allow-Origin", "*")
                .header("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
                .header("Access-Control-Allow-Headers", "Content-Type,X-Amz-Date,Authorization,X-Api-Key,X-Amz-Security-Token,X-Amz-User-Agent")
                .entity(response)
                .build();    }
}
