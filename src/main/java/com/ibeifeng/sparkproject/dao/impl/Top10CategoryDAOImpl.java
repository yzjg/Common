package com.ibeifeng.sparkproject.dao.impl;

import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.jdbc.JDBCHelper;

/**
 * Created by Administrator on 2017/3/28.
 */
public class Top10CategoryDAOImpl  implements ITop10CategoryDAO {

    public void insert(Top10Category category){
          String sql="insert into top10_category values(?,?,?,?,?)";
          Object[] param=new Object[]{
                  category.getTaskid(),
                  category.getCategoryid(),
                  category.getClickCount(),
                  category.getOrderCount(),
                  category.getPayCount()};\
        JDBCHelper jdbcHelper=JDBCHelper.getInstance();
        jdbcHelper.executeUpdate(sql,param);

    }
}
