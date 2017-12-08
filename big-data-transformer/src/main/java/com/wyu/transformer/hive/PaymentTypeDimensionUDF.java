package com.wyu.transformer.hive;

import com.wyu.commom.GlobalConstants;
import com.wyu.transformer.model.dim.base.PaymentTypeDimension;
import com.wyu.transformer.service.rpc.IDimensionConverter;
import com.wyu.transformer.service.rpc.client.DimensionConverterClient;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.UDF;

import java.io.IOException;



/**
 * 订单支付方式dimension对应的udf
 * 
 * @author gerry
 *
 */
public class PaymentTypeDimensionUDF extends UDF {
    private IDimensionConverter converter = null;

    public PaymentTypeDimensionUDF() {
        try {
            this.converter = DimensionConverterClient.createDimensionConverter(new Configuration());
        } catch (IOException e) {
            throw new RuntimeException("创建converter异常");
        }

        // 添加一个钩子进行关闭操作
        Runtime.getRuntime().addShutdownHook(new Thread(new Runnable() {

            @Override
            public void run() {
                try {
                    DimensionConverterClient.stopDimensionConverterProxy(converter);
                } catch (Throwable e) {
                    // nothing
                }
            }
        }));
    }

    /**
     * 根据给定的payment方式名称，返回对应的id值
     * 
     * @param paymentType
     * @return
     */
    public int evaluate(String paymentType) {
        paymentType = StringUtils.isBlank(paymentType) ? GlobalConstants.DEFAULT_VALUE : paymentType.trim();
        PaymentTypeDimension dimension = new PaymentTypeDimension(paymentType);
        try {
            return this.converter.getDimensionIdByValue(dimension);
        } catch (IOException e) {
            throw new RuntimeException("获取id异常");
        }
    }
}
