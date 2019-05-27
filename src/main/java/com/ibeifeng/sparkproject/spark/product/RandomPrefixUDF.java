package com.ibeifeng.sparkproject.spark.product;

/**
 * Created by Administrator on 2017/4/1.
 */
public class RandomPrefixUDF  implements UDF2<String,Integer,String> {
    public String  call(String  val,Integer  num){
         Random  random=new Random();
         int randNum=random.nextInt(10);
         return  randNum+"_"+val;
    }
}
