import com.wyu.transformer.model.dim.base.EventDimension;
import com.wyu.transformer.service.rpc.IDimensionConverter;
import com.wyu.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class TestRPC {
    public static void main(String[] args) throws IOException {
//        DimensionConverterServer.main(args);

        IDimensionConverter converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        System.out.println(converter);
        System.out.println(converter.getDimensionIdByValue(new EventDimension()));
        DimensionConverterClient.stopDimensionConverterProxy(converter);
    }
}
