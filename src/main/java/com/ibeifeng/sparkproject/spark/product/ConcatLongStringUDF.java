package com.ibeifeng.sparkproject.spark.product;

/**
 * Created by Administrator on 2017/4/1.
 */
public class ConcatLongStringUDF  implements UDF3<Long,String,String,String> {
     public String  call(Long v1,String v2,String split) throws Exception{
         return  String.valueOf(v1)+split+v2;
     }
}
