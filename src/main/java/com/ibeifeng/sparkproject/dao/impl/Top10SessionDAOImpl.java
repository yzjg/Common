package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10SessionDAO;
import com.ibeifeng.sparkproject.domain.Top10Session;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/3/29.
 */
public class Top10SessionDAOImpl  implements ITop10SessionDAO {
    public  void insert(Top10Session top10Session){
        String sql="insert into top10_session  value(?,?,?,?,?)";
        Object[]  param=new Object[]{
                    top10Session.getTaskid(),
                     top10Session.getSessionid(),
                     top10Session.getCategoryid(),
                      top10Session.getClickCount()};
           JDBCHelper jdbcHelper= JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,param);
    }

}
