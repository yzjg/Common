package com.ibeifeng.sparkproject.util;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;

/**
 * Created by Administrator on 2017/3/26.
 */
public class DateUtils {
     public static final SimpleDateFormat TIME_FORMAT=new SimpleDateFormat("yyyy-MM-dd  HH:mm:ss");
     public static final  SimpleDateFormat  DATE_FORMAT=new SimpleDateFormat("yyyy-MM-dd");
     public static final   SimpleDateFormat   DATEKEY_FORMAT=new SimpleDateFormat("yyyyMMdd");
     public static  String   formatDateKey(Date date){
         return   DATEKEY_FORMAT.format(date);
     }
     public static boolean before(String time1,String time2){
         try {
             Date dateTime1=TIME_FORMAT.parse(time1);
             Date  dateTime2=TIME_FORMAT.parse(time2);
              if(dateTime1.before(dateTime2)){
                  return true;
              }

         } catch (ParseException e) {
             e.printStackTrace();
         }
        return false;
     }
   public static boolean  after(String time1,String  time2) {

       try {
           Date    dateTime1=TIME_FORMAT.parse(time1);
           Date  dateTime2=TIME_FORMAT.parse(time2);
           if(dateTime1.after(dateTime2)){
               return true;
           }

       } catch (ParseException e) {
           e.printStackTrace();
       }
      return false;
   }
  public static int minus(String time1,String time2){
      try {
          Date  dateTime1=TIME_FORMAT.parse(time1);
          Date  dateTime2=TIME_FORMAT.parse(time2);
          long  millisecond=dateTime1.getTime()-dateTime2.getTime();
          return  Integer.valueOf(String.valueOf(millisecond/1000));
      } catch (ParseException e) {
          e.printStackTrace();
      }
        return  0;
  }

   public static String getDateHour(String datetime){
         String date=datetime.split(" ")[0];
        String hourMinuteSecond=datetime.split(" ")[1];
        String hour=hourMinuteSecond.split(":")[0];
        return date+"_"+hour;
   }
   public static   String getTodayDate(){
       return DATE_FORMAT.format(new Date());
   }
   public static String getYesterdayDate(){
       Calendar cal=Calendar.getInstance();
       cal.setTime(new Date());
       cal.add(Calendar.DAY_OF_YEAR ,-1);
        Date    date=cal.getTime();
        return  DATE_FORMAT.format(date);
   }
   public static String formatDate(Date date){
       return DATE_FORMAT.format(date);
   }

   public static String formatTime(Date date){
       return TIME_FORMAT.format(date);
   }
   public static Date parseTime(String time){
       try {
           return TIME_FORMAT.parse(time);
       } catch (ParseException e) {
           e.printStackTrace();
       }
       return null;
   }

   public static  Date  parseDateKey(String dateKey){
       try {
           return   DATEKEY_FORMAT.parse(dateKey);
       } catch (ParseException e) {
           e.printStackTrace();
       }
       return  null;
   }
 public static String formatTimeMinute(Date date){
       SimpleDateFormat sdf=new SimpleDateFormat("yyyyMMddHHmm");
       return sdf.format(date);
 }


}
