import org.eclipse.microprofile.health.*;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableRequest;
import software.amazon.awssdk.services.dynamodb.model.DescribeTableResponse;
import software.amazon.awssdk.services.dynamodb.model.DynamoDbException;
import software.amazon.awssdk.services.dynamodb.model.ProvisionedThroughputDescription;

import javax.enterprise.context.ApplicationScoped;
import javax.inject.Inject;

@ApplicationScoped
@Liveness
@Readiness
public class DynamoDbHealthCheck implements HealthCheck {

    private DynamoDbClient dynamoDB;
    private String REQUIRED_TABLE;
    @Inject
    private ConfigProperties configProperties;

    @Override
    public HealthCheckResponse call() {
        this.dynamoDB = DynamoDbClient.builder()
                .region(Region.of(configProperties.getDynamoRegion()))
                .build();
        this.REQUIRED_TABLE = configProperties.getTableName();
        HealthCheckResponseBuilder responseBuilder = HealthCheckResponse.named("DynamoDB health check");
        try {
            DescribeTableResponse describeTableResponse = dynamoDB.describeTable(DescribeTableRequest.builder().tableName(REQUIRED_TABLE).build());
            ProvisionedThroughputDescription throughput = describeTableResponse.table().provisionedThroughput();

            if (throughput.readCapacityUnits() < 1 || throughput.writeCapacityUnits() < 1) {
                return responseBuilder.down()
                        .withData("error", "Table " + REQUIRED_TABLE + " has insufficient read/write capacity")
                        .withData("tableName", REQUIRED_TABLE)
                        .withData("readCapacityUnits", throughput.readCapacityUnits())
                        .withData("writeCapacityUnits", throughput.writeCapacityUnits())
                        .build();
            }

            return responseBuilder.up()
                    .withData("tableName", REQUIRED_TABLE)
                    .withData("readCapacityUnits", throughput.readCapacityUnits())
                    .withData("writeCapacityUnits", throughput.writeCapacityUnits())
                    .build();
        } catch (DynamoDbException e) {
            return responseBuilder.down()
                    .withData("error", "Table " + REQUIRED_TABLE + " does not exist or another error occurred")
                    .withData("tableName", REQUIRED_TABLE)
                    .build();
        }
    }
}
