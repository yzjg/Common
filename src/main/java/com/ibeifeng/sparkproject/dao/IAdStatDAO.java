package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdStat;

import java.util.List;

/**
 * Created by Administrator on 2017/4/2.
 */
public interface IAdStatDAO {
    void  updateBatch(List<AdStat> adStats);
}
