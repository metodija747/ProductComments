import org.eclipse.microprofile.rest.client.inject.RegisterRestClient;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

@Path("/products")
@RegisterRestClient
public interface ProductCatalogApi {

    @PUT
    @Path("/{productId}")
    @Consumes(MediaType.APPLICATION_JSON)
    Response updateProductRating(@PathParam("productId") String productId,
                                 @QueryParam("action") String action,
                                 @HeaderParam("Authorization") String authHeader,
                                 double avgRating);
}

