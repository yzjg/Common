package com.ibeifeng.sparkproject.util;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

/**
 * Created by Administrator on 2017/3/31.
 */
public class SparkUtils {
    public static void setMaster(SparkConf conf) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            conf.setMaster("local");
        }
    }

    public static void mockData(JavaSparkContext sc, SQLContext sqlContext) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (local) {
            MockData.mock(sc, sqlContext);
        }
    }

    public static SQLContext getSQLContext(SparkContext sc) {
        boolean local = ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if (lcoal) {
            return new SQLContext(sc);
        } else {
            return new HiveContext(sc);
        }
    }
    public  static  JavaRDD<Row>  getActionRDDByDateRange(SQLContext   sqlContext,JSONObject  taskParam){
        String  startDate=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
        String  endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        String  sql="select * from user_visit_action   where date>='"+startDate+"'"
                +"  and  date<='"+endDate+"'";
          DataFrame actionDF=sqlContext.sql(sql);
          return  actionDF.javaRDD();
    }




}