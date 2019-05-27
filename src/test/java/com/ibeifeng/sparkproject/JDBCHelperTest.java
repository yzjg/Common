package com.ibeifeng.sparkproject;

import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by Administrator on 2017/3/27.
 */
public class JDBCHelperTest {
    public static void main(String[] arags) throws Exception{
        JDBCHelper  jdbcHelper= JDBCHelper.getInstance();
        final Map<String,Object> testUser=new HashMap<String,Object>();
        jdbcHelper.executeUpdate("insert into test_user(name,age) values(?,?)",new Object[]{'您',23});
        jdbcHelper.executeQuery("select  name ,age from test_user where id=?",new Object[]{5},new JDBCHelper.QueryCallback(){
            public void process(ResultSet rs) throws Exception {
                if(rs.next()){
                 String  name=rs.getString(1);
                 int age=rs.getInt(2);
                  testUser.put("name",name);
                  testUser.put("age",age  );
                }
            }
        });
        //批量进行的sql语句操作
        String sql="insert into test_user(name,age) values(?,?)";
        List<Object[]> paramsList=new ArrayList<Object[]>();
        paramsList.add(new Object[]{"ni",23});
        paramsList.add(new Object[]{"nhklao",34});
        paramsList.add(new Object[]{"ni",34});
        jdbcHelper.executeBatch(sql,paramsList);
    }





}
