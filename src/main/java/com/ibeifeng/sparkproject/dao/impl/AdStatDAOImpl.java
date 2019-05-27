package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.domain.AdStat;
import com.ibeifeng.sparkproject.domain.AdStatQueryResult;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/4/2.
 */
public class AdStatDAOImpl   implements IAdStatDAO {
    public  void updateBatch(List<AdStat> adStats){
              JDBCHelper jdbcHelper=JDBCHelper.getInstance();
              List<AdStat>  insertAdStats=new ArrayList<AdStat>();
              List<AdStat>   updateAdStats=new  ArrayList<AdStat>();
             String  selectSQL="select  count(*) from ad_stat where  date=?"
                     +"  and province=?  and city=?  and ad_id=?";
             for(AdStat adStat:adStats){
                 final AdStatQueryResult queryResult=new AdStatQueryResult();
                 Object[]  params=new Object[]{
                         adStat.getDate(),adStat.getProvince(),adStat.getCity(),
                         adStat.getAdid()};
                 jdbcHelper.executeUpdate(selectSQL,params,new JDBCHelper.QueryCallback(){
                     public   void process(ResultSet rs){
                              if(rs.next()){
                                  int count=rs.getInt(1);
                                  queryResult.setCount(count);
                              }
                     }
                 });
               int count=queryResult.getCount();
               if(count>0){
                   updateAdStats.add(adStat);
               }
               else {
                   insertAdStats.add(adStat);
               }
             }
               String  insertSQL="insert  into  ad_stat values(?,?,?,?,?)";
               List<Object[]>    insertParamsList=new ArrayList<Object[]>();
               for(AdStat  adStat:adStats){
                   Object[]  params=new Object[]{
                           adStat.getDate(),adStat.getProvince(),adStat.getCity(),
                           adStat.getAdid(),adStat.getClickCount()};
                   insertParamsList.add(params);
                }
        jdbcHelper.executeBatch(insertSQL,insertParamsList);
        String updateSQL="update ad_stat set click_count=? from  ad_stat"
                +" where date=?   and province=? and city=?  and ad_id=?";
         List<Object[]> updateParamsList=new ArrayList<Object[]>();
         for(AdStat adStat:updateAdStats){
             Object[]  params=new Object[]{adStat.getClickCount(),adStat.getDate(),
             adStat.getProvince(),adStat.getCity(),adStat.getAdid()};
             updateParamsList.add(params);
          }
         jdbcHelper.executeBatch(updateSQL,updateParamsList);

    }
}
