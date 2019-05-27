package com.ibeifeng.sparkproject.util;

/**
 * Created by Administrator on 2017/3/26.
 */
public class ParamUtils {
    public static Long  getTaskFromArgs(String[] args,String  taskTpe){
        boolean  local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            return  ConfigurationManager.getLong(taskType);
        }
        else {
            try {
                if (args != null && args.length > 0) {
                    return Long.valueOf(args[0]);
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        return  null;
    }
    public static String getParam(JSONObject  jsonObject,String  field){
        JSONObject  jsonArray=jsonObject.getJSONArray(field);
        if(jsonArray!=null && jsonArray .size()>0){
            return jsonArray.getString(0);
        }
        return null;
    }

}
