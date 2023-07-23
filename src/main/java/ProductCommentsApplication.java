import com.kumuluz.ee.discovery.annotations.RegisterService;

import javax.ws.rs.ApplicationPath;
import javax.ws.rs.core.Application;
import java.util.logging.Logger;

@ApplicationPath("/")
@RegisterService
public class ProductCommentsApplication extends Application{
    private static final Logger LOG = Logger.getLogger(ProductCommentsApplication.class.getName());
    public ProductCommentsApplication() {
        LOG.info("ProductCommentsApplication started!");
    }
}

