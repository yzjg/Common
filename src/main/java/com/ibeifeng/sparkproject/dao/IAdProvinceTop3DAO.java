package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdProvinceTop3;

import java.util.List;

/**
 * Created by Administrator on 2017/4/2.
 */
public interface IAdProvinceTop3DAO   {
    void  updateBatch(List<AdProvinceTop3> adProvinceTop3s);
}
