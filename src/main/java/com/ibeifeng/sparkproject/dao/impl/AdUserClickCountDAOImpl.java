package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.domain.AdUserClickCount;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import com.ibeifeng.sparkproject.model.AdUserClickCountQueryResult;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/4/2.
 */
public class AdUserClickCountDAOImpl  implements IAdUserClickCountDAO {
      public void  updateBatch(List<AdUserClickCount>  adUserClickCounts){
           JDBCHelper  jdbcHelper= JDBCHelper.getInstance();
           List<AdUserClickCount> insertAdUserClickCounts=new ArrayList<AdUserClickCount>();
           List<AdUserClickCount> updateAdUserClickCounts=new ArrayList<AdUserClickCount>();
           String   selectSQL="SELECT count(*) from  ad_user_click_count where  date=? and  user_id=?  and  ad_id=?";
           Object[]  selectParams=null;
           for(AdUserClickCount   adUserClickCount:adUserClickCounts){
               final AdUserClickCountQueryResult queryResult=new AdUserClickCountQueryResult();
               selectParams=new Object[]{
                           adUserClickCount.getDate(),
                           adUserClickCount.getUserid(),
                           adUserClickCount.getAdid()};
                jdbcHelper.executeUpdate(selectSQL,selectParams,new JDBCHelper.QueryCallback(){
                    public  void process(ResultSet rs){
                             if(rs.next()){
                                 int  count=rs.getInt(1);
                                 queryResult.setCount(count);
                             }
                    }
                });
                 int  count=queryResult.getCount();
                 if(count>0){
                      updateAdUserClickCounts.add(adUserClickCount);
                 }
                 else {
                     insertAdUserClickCounts.add(adUserClickCount);
                 }
           }
           String  insertSQL="insert into  ad_user_click_count values(?,?,?,?)";
           List<Object[]>  insertParamsList=new ArrayList<Object[]>();
           for(AdUserClickCount  adUserClickCount:insertAdUserClickCounts){
               Object[]  insertParams=new Object[]{
                       adUserClickCount.getDate(),adUserClickCount.getUserid(),
                       adUserClickCount.getAdid(),adUserClickCount.getClickCount()};
                  insertParamsList.add(insertParams);
           }
          jdbcHelper.executeBatch(insertSQL,insertParamsList);
          String   updateSQL="update  ad_user_click_count  set  click_count=?"
                  +" where  date=? and  user_id=?  and  ad_id=?";
          List<Object[]>  updateParamsList=new ArrayList<Object[]>();
          for(AdUserClickCount  adUserClickCount:updateAdUserClickCounts){
              Object[]  updateParams=new Object[]{adUserClickCount.getClickCount(),
              adUserClickCount.getDate(),adUserClickCount.getUserid(),
              adUserClickCount.getAdid()};
              updateParamsList.add(updateParams);
              }
          jdbcHelper.executeBatch(updateSQL,updateParamsList);




      }}
