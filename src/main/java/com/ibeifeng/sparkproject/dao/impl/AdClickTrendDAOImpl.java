package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IAdClickTrendDAO;
import com.ibeifeng.sparkproject.domain.AdClickTrend;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;
import com.ibeifeng.sparkproject.model.AdClickTrendQueryResult;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2017/4/2.
 */
public class AdClickTrendDAOImpl  implements IAdClickTrendDAO {
    public void updateBatch(List<AdClickTrend> adClickTrends){
        JDBCHelper jdbcHelper= JDBCHelper.getInstance();
        List<AdClickTrend>   updateAdClickTrends=new ArrayList<AdClickTrend>();
        List<AdClickTrend>   insertAdClickTrends=new ArrayList<AdClickTrend>();
        String selectSQL="select  count(*)  from  ad_click_trend  where date=?  and hour=?  and minute=?   and ad_id=?";
        for(AdClickTrend adClickTrend:adClickTrends){
            final AdClickTrendQueryResult queryResult=new AdClickTrendQueryResult();
            Object[]  params=new Object[]{
                    adClickTrend.getDate(),adClickTrend.getHour(),adClickTrend.getMinute(),
                    adClickTrend.getAdid()};
            jdbcHelper.executeUpdate(selectSQL,params,new JDBCHelper.QueryCallback(){
                public void process(ResultSet rs){
                    if(rs.next()){
                        int count=rs.getInt(1);
                        queryResult.setCount(count);
                    }}
            });
            int count=queryResult.getCount();
            if(count>0){
                updateAdClickTrends.add(adClickTrend);
            }else {
                insertAdClickTrends.add(adClickTrend);
            }
        }
        String updateSQL="update ad_click_trend   set   click_count=? " +
                "  where date=?  and hour=? and minute=? and  ad_id=? ";
        List<Object[]>  updateParamsList=new ArrayList<Object[]>();
        for(AdClickTrend adClickTrend:updateAdClickTrends){
            Object[] params=new Object[]{
                    adClickTrend.getDate(),adClickTrend.getHour(),adClickTrend.getMinute(),
                    adClickTrend.getAdid()};
                updateParamsList.add(params);
        }
        jdbcHelper.executeBatch(updateSQL,updateParamsList);
        String insertSQL="insert into ad_click_trend values(?,?,?,?,?)";
        List<Object[]>   insertParamsList=new ArrayList<Object[]>();
         for(AdClickTrend adClickTrend : insertAdClickTrends){
             Object[]  params=new Object[]{
                     adClickTrend.getDate(),adClickTrend.getHour(),adClickTrend.getMinute(),
                     adClickTrend.getAdid(),adClickTrend.getClickCount()};
                   insertParamsList.add(params);
         }
      jdbcHelper.executeUpdate(insertSQL,insertParamsList);
    }
}
