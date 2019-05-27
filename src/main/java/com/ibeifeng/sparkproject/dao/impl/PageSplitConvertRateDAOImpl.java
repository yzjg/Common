package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ibeifeng.sparkproject.domain.PageSplitConvertRate;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/3/31.
 */
public class PageSplitConvertRateDAOImpl  implements IPageSplitConvertRateDAO {
    public void insert(PageSplitConvertRate pageSplitConvertRate){
        String sql="insert  into page_split_convert_rate  values(?,?)";
        Object[]    params=new Object[]{
                pageSplitConvertRate.getTaskid(),
                pageSplitConvertRate.getConvertRate()
        };
      JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,params);


    }
}
