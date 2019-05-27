package com.ibeifeng.sparkproject.util;

/**
 * Created by Administrator on 2017/3/26.
 */
public class StringUtils {
    public static boolean  isEmpty(String str){
        return str==null || "".equals(str);
    }
   public static    boolean  isNotEmpty(String str){
        return  str!=null  ||!"".equals(str);
   }
   public static String     trimComma(String str){
       if(str.startsWith(",")){
           str=str.substring(1);
       }
       if(str.endsWith(",")){
           str=str.substring(0,str.length()-1);
       }
       return str;
   }


   public   static String fulfuill(String str){
       if(str.length()==2){
           return  str;
       }
       else{
           return  "0"+str;
       }
   }

   public static String     getFieldFromConcatString(String str,String delimiter,String field){
       String[]     fields=str.split(delimiter);
        for(String concatField:fields){
            // searchKeywords=|clickCategoryIds=1,2,34
            if(concatField.split("=").length==2) {
                String fieldName = concatField.split("=")[0];
                String fieldValue = concatField.split("=")[1];
                if (fieldName.equals(field)) {
                    return fieldValue;
                }
            }
        }
        return null;
   }
 public static String   setFieldInConcatString(String str,String delimiter,String field,  String newFieldValue){
       String[] fields=str.split(delimiter);
       for(int i=0;i<fields.length;i++){
           String fieldName=fields[i].split("=")[0];
           if(fieldName.equals(field)){
               String   concatField=fieldName+"="+newFieldValue;
               fields[i]=concatField;
               break;
           }
       }
        StringBuffer  buffer=new StringBuffer("") ;
        for(int i=0;i<fields.length;i++){
            buffer.append(fields[i]);
            if(i<fields.length-1){
                buffer.append("|");
            }
        }
    return  buffer.toString();
 }



}
