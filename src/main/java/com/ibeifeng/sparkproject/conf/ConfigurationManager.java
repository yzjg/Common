package com.ibeifeng.sparkproject.conf;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

/**
 * Created by Administrator on 2017/3/26.
 */
public class ConfigurationManager {

    private static Properties prop=new Properties();
    static{
        try {
            InputStream in=ConfigurationManager.class.getClassLoader().getResourceAsStream("my.properties");
            prop.load(in);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static String getProperty(String key){
        return prop.getProperty(key);
    }
   public static Integer  getInteger(String key){
        String value=getProperty(key);
        try{
            return  Integer.valueOf(value);
        }
        catch(Exception  e){e.printStackTrace();}
        return  0;
   }
   public static Boolean  getBoolean(String key){
       String value=getProperty(key);
       try{
           return   Boolean.valueOf(value);
       }
       catch(Exception  e){e.printStackTrace();}
       return  false;
   }


   public static Long   getLong(String  key){
       String value=getProperty(key);
       try{
           return  Long.valueOf(value);
       }
       catch(Exception   e){
           e.printStackTrace();
       }
       return  0L;
   }

}
