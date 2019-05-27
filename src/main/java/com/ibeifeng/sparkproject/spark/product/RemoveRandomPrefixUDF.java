package com.ibeifeng.sparkproject.spark.product;

/**
 * Created by Administrator on 2017/4/1.
 */
public class RemoveRandomPrefixUDF  implements  UDF1<String,String> {
    public   String call(String val){
        return   val.split("_")[1];
    }
}
