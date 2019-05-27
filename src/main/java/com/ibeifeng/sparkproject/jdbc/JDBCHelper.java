package com.ibeifeng.sparkproject.jdbc;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;

/**
 * Created by Administrator on 2017/3/27.
 */
public class JDBCHelper {
    static{
        try{
            String  driver= ConfigurationManager.getProperty(Constants.JDBC_DRIVER);
            Class.forName(driver);
        }catch(Exception e){e.printStackTrace();}
    }
   private static JDBCHelper instance=null;

    public static JDBCHelper  getInstance(){
        if(instance==null){
                synchronized(JDBCHelper.class){
                    if(instance==null){
                        instance=new JDBCHelper();
                    }
                }
        }
        return  instance;
    }
    private LinkedList<Connection> datasource=new LinkedList<Connection>();
    private JDBCHelper(){
       int datasourceSize=ConfigurationManager.getInteger(Constants.JDBC_DATASOURCE_SIZE);
        for(int i=0;i<datasourceSize;i++){
            boolean local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
            String url=null;
            String user=null;
            String  password=null;
            if(local){
               url=ConfigurationManager.getProperty(Constants.JDBC_URL);
               user=ConfigurationManager.getProperty(Constants.JDBC_USER);
               password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
            }else{
                url=ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
                user=ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
                password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
            }

            try {
                Connection  conn= DriverManager.getConnection(url,user,password);
                datasource.push(conn);
              //  datasource.push(conn);
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
    }
   public synchronized Connection getConnection(){
        while(datasource.size()==0){
            try {
                Thread.sleep(1000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        return  datasource.poll();
   }


   public int executeUpdate(String sql,Object[] params){
       int rtn=0;
       Connection conn=null;
       PreparedStatement pstmt=null;
       try{
          conn=getConnection();
          pstmt=conn.prepareStatement(sql);
          if(params !=null &&   params.length>0) {
              for (int i = 0; i < params.length; i++) {
                  pstmt.setObject(i + 1, params[i]);
              }
          }
          rtn=pstmt.executeUpdate();
       }catch(Exception  e){
           e.printStackTrace();
       }
       finally{
           try{
               if(pstmt!=null){
                   pstmt.close();
           }
               if(conn!=null){
                   datasource.push(conn );
               }
           }catch(Exception  e2){e2.printStackTrace();}
       }
       return rtn;
   }
   public void executeQuery(String sql,Object[] params,QueryCallback  callback){
          Connection  conn=null;
          PreparedStatement pstmt=null;
          ResultSet rs= null;
          try{
              conn=getConnection();
              pstmt=conn.prepareStatement(sql);
               for(int i=0;i<params.length;i++){
                   pstmt.setObject(i+1,params[i]);
               }
              rs=pstmt.executeQuery();
              callback.process(rs);
          }catch(Exception  e){
              e.printStackTrace();
          }
          finally{
              try{
                  if(conn!=null){
                      datasource.push(conn);
                  }
              }
              catch(Exception  e2){
                  e2.printStackTrace();
              }
          }
   }
 public   static interface  QueryCallback{
       public  void   process(ResultSet rs) throws Exception;
   }
   public     int[]   executeBatch(String sql,List<Object[]> paramsList){
       int[] rtn= null;
       Connection conn=null;
       PreparedStatement   pstmt=null;`
       try{

       }catch(Exception  e){e.printStackTrace();}
       finally{
           try{
               if(conn!=null){
                   datasource.push(conn);
               }
           }
           catch(Exception e2){
               e2.printStackTrace();
           }
       }
       return  rtn;
   }

}
