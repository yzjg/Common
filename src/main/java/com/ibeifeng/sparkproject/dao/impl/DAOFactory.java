package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.*;

/**
 * Created by Administrator on 2017/3/27.
 */
public class DAOFactory {
   public static ITaskDAO getTaskDAO(){
       return   new TaskDAOImpl();
   }
   public  static ISessionAggrStatDAO getSessionAggrStatDAO(){
       return  new  SessionAggrStatDAOImpl();
   }
    public static ISessionRandomExtractDAO getSessionRandomExtractDAO(){
       return new SessionRandomExtractDAOImpl();
    }
    public static ISessionDetailDAO getSessionDetailDAO(){
        return new  SessionDetailDAOImpl();
    }
    public  static ITop10CategoryDAO getTop10CategoryDAO(){
        return new Top10CategoryDAOImpl();
    }

     public  static  ITop10SessionDAO  getTop10SessionDAO(){
        return new   Top10SessionDAOImpl();
     }
     public static IPageSplitConvertRateDAO  getPageSplitConvertRateDAO(){
         return  new PageSplitConvertRateDAOImpl();
     }
     public  static  IAdUserClickCountDAO  getAdUserClickCountDAO(){
         return  new  AdUserClickCountDAOImpl();
     }
     public static  IAdStatDAO  getAdStatDAO(){
         return  new AdStatDAOImpl();
     }
     public static  IAdProvinceTop3DAO  getAdProvinceTop3DAO(){
         return new  AdProvinceTop3DAOImpl();
     }
     public static IAdClickTrendDAO getAdClickTrendDAO(){
         return  new AdClickTrendDAOImpl();
     }

}
