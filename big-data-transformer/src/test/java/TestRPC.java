import com.wyu.transformer.model.dim.base.CurrencyTypeDimension;
import com.wyu.transformer.service.rpc.IDimensionConverter;
import com.wyu.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;

public class TestRPC {
    public static void main(String[] args) throws IOException {
//        DimensionConverterServer.main(args);
        System.setProperty("hadoop.home.dir", "D:\\开发资源\\开发资源文件\\CDH5.3.6\\hadoop-2.5.0-cdh5.3.6");
        IDimensionConverter converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        System.out.println(converter);
        System.out.println(converter.getDimensionIdByValue(new CurrencyTypeDimension("RMB")));
        DimensionConverterClient.stopDimensionConverterProxy(converter);
    }
}
