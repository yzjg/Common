package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/3/28.
 */
public class SessionRandomExtractDAOImpl  implements ISessionRandomExtractDAO {
    public void insert(SessionRandomExtract sessionRandomExtract){
         String sql="insert into session_random_extract  values(?,?,?,?,?)";
         Object[]  params=new Object[]{
                 sessionRandomExtract.getTaskid(),
                 sessionRandomExtract.getSessionid(),
                 sessionRandomExtract.getStartTime(),
                 sessionRandomExtract.getSearchKeywords(),
                 sessionRandomExtract.getClickCategoryIds()
         };
         JDBCHelper jdbcHelper= JDBCHelper.getInstance();
         jdbcHelper.executeUpdate(sql,params);





    }
}
