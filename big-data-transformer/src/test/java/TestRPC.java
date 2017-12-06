import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import com.wyu.transformer.model.dim.base.PlatformDimension;
import com.wyu.transformer.service.rpc.IDimensionConverter;
import com.wyu.transformer.service.rpc.client.DimensionConverterClient;

public class TestRPC {
    public static void main(String[] args) throws IOException {
//        DimensionConverterServer.main(args);

        IDimensionConverter converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        System.out.println(converter);
        System.out.println(converter.getDimensionIdByValue(new PlatformDimension("test")));
        DimensionConverterClient.stopDimensionConverterProxy(converter);
    }
}
