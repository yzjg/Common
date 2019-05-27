package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/4/2.
 */
public class AdBlacklistDAOImpl   implements  IAdBlacklistDAO {
    public   void insertBatch(List<AdBlacklist>  adBlackLists){
        String   sql="insert  into  ad_blacklist  values(?)";
        List<Object[]>  paramsList=new ArrayList<Object[]>();
        for(AdBlacklisit  adBlacklist:adBlackLists){
            Object[]   params=new Object[]{adBlacklist.getUserid()};
            paramsList.add(params);
        }
        JDBCHelper  jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeBatch(sql,paramsList);
    }
    public  List<AdBlacklist>   find(){
        String  sql="select  *  from  ad_blacklist";
        final   List<AdBlacklist>  adBlacklists=new ArrayList<AdBlacklist>();
        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeQuery(sql,null,new JDBCHelper.QueryCallback(){
            public void process(ResultSet rs){
                while(rs.next()){
               long userid=Long.valueOf(String.valueOf(rs.getInt(1)));
               AdBlacklist   adBlacklist=new AdBlacklist();
               adBlacklist.setUserid(userid);
               adBlacklists.add(adBlacklist);}
            }
        });
        return   adBlacklists;


    }
}
