package com.ibeifeng.sparkproject.spark.product;

import java.util.Arrays;

/**
 * Created by Administrator on 2017/4/1.
 */
public class GroupConcatDistinctUDA  extends   UserDefinedAggregateFunction{
   private  StructType  inputSchema=DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("cityInfo",DataTypes.StringType,true))));
   private StructType    bufferSchema=DataTypes.createStructType(Arrays.asList(DataTypes.createStructField("bufferCityInfo",DataTypes.StringType,true)));
   private   DataType dataType=DataTypes.StringType;
   private  boolean  deterministic=true;
   public StructType     inputSchema(){
        return inputSchema;
   }
   public  StructType  bufferSchema(){
       return  bufferSchema();
   }
   public   DataType  dataType(){
       return  dataType;
   }
   public boolean   deterministic(){
       return  deterministic;
   }
   public void initialize(MutableAggregationBuffer buffer){
       buffer.update(0,"");
   }
  public   void  update(MutableAggregationBuffer buffer,Row input){
          String bufferCityInfo=buffer.getString(0);
          String cityInfo=input.getString(0);
          if(!bufferCityInfo.contains(cityInfo)){
               if("".equals(bufferCityInfo)){
                   bufferCityInfo+=cityInfo;
               }
          else{
                bufferCityInfo+=","+cityInfo;
          }}
         buffer.update(0,bufferCityInfo);
  }
 public void merge(MutableAggregationBuffer buffer1,Row buffer2){
      String    bufferCityInfo1=buffer1.getString(0);
      String    bufferCityInfo2=buffer2.getString(0);
      for(String  cityInfo:bufferCityInfo2.split(",")){
            if(!bufferCityInfo1.contains(cityInfo)){
                   if("".equals(bufferCityInfo1)){
                       bufferCityInfo1+=cityInfo;
                   }
                   else{}
                   bufferCityInfo1+=","+cityInfo;
            }
      }
                  buffer1.update(0,bufferCityInfo1);
 }
 public Object evaluate(Row row){
     return  row.getString(0);
 }





}
