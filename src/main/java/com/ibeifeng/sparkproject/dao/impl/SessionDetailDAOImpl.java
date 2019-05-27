package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/3/28.
 */
public class SessionDetailDAOImpl   implements ISessionDetailDAO {
    public void insert(SessionDetail sessionDetail){
        String sql="insert  into    session_detail  values(?,?,?,?,?,?,?,?,?,?,?,?,)";
        Object[] params=new Object[]{
                sessionDetail.getTaskid(),
                sessionDetail.getUserid(),
                sessionDetail.getSessionid(),
                sessionDetail.getPageid(),
                sessionDetail.getActionTime(),
                sessionDetail.getSearchKeyword(),
                sessionDetail.getClickCategoryId(),
                sessionDetail.getClickProductId(),
                sessionDetail.getOrderCategoryIds(),
                sessionDetail.getOrderProductIds(),
                sessionDetail.getPayCategoryIds(),
                sessionDetail.getPayProductIds()
        };
        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);


    }
}
