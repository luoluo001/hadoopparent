package com.lcy.hive.udf;


import com.lcy.hive.udf.bean.MovieRateBean;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.codehaus.jackson.map.ObjectMapper;

import java.io.IOException;

/**
 * Created by： luochengyue
 * date: 2019/7/4.
 * desc：
 */
public class JsonHandUdf extends UDF{

    //hive可实现自定义udf，实现UDF,UDFA,UDFT三种形式自定义函数
    public String evaluate(final String jsonLine) {
        ObjectMapper objectMapper = new ObjectMapper();
        MovieRateBean mBean = null;
        try {
            mBean = objectMapper.readValue(jsonLine, MovieRateBean.class);
            return mBean.toString();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return "";
    }

    public static void main(String[] args){
        String jsonLine = "{\"movie\":\"1721\",\"rate\":\"3\",\"timeStamp\":\"965440048\",\"uid\":\"5114\"}";
        JsonHandUdf jsonHandUdf = new JsonHandUdf();
        String beanString = jsonHandUdf.evaluate(jsonLine);
        System.out.println(beanString);
    }
}
