package com.ibeifeng.sparkproject;

/**
 * Created by Administrator on 2017/3/27.
 */
public class FastjsonTest {
  public static void main(String[] args){
     String json="[{'学生':'你','班级':'3','年纪':'34','科目':'导师','成绩':'233'}," +
        "{'学生223':'你233','班级332':'31233','年纪23':'34232','科目2332':'导师2','成绩':'233'},{}]";
     JSONArray jsonArray=   JSONArray.parseArray(json);
     JSONObject  jsonObject= jsonArray.getJSONObject(0);
       jsonObject.getString();
               }
               }

