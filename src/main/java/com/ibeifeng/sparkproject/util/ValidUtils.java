package com.ibeifeng.sparkproject.util;

/**
 * Created by Administrator on 2017/3/26.
 */
public class ValidUtils {
    public static   boolean  between(String data,String dataField,
                                     String parameter,String startParamField,String  endParamField){
             String     startParamFieldStr=StringUtils.getFieldFromConcatString(parameter,"\\|",startParamField);
            String endParamFieldStr=StringUtils.getFieldFromConcatString(parameter,"\\|",endParamField);
          if(startParamFieldStr==null || endParamFieldStr==null){
              return true;
          }
          int startParamFieldValue=Integer.valueOf(startParamFieldStr);
          int endParamFieldValue=Integer.valueOf(endParamFieldStr);
          String dataFieldStr=StringUtils.getFieldFromConcatString(data,"\\|",dataField);
          if(dataFieldStr!=null){
              int dataFieldValue=Integer.valueOf(dataFieldStr);
              if(dataFieldValue>=startParamFieldValue&&dataFieldValue<=endParamFieldValue){return true;}
              else{return false;}
          }
                   return false;
    }


    public static boolean in(String data,String dataField,String parameter,String paramField){
        String paramFieldValue=StringUtils.getFieldFromConcatString(parameter,"\\|",paramField);
        if(paramFieldValue==null){
            return  true;
        }
        return false;
    }

    public static boolean  equal(String data,String dataField,String  parameter,String  paramField){
        String paramFieldValue=StringUtils.getFieldFromConcatString(parameter,"\\|",paramField);
        if(paramFieldValue==null){return true;}
       String dataFieldValue=StringUtils.getFieldFromConcatString(data,"\\|",dataField);
        if(dataFieldValue!=null){
            if(dataFieldValue.equals(paramFieldValue)){
                return true;
            }
        }
        return false;
    }


}
