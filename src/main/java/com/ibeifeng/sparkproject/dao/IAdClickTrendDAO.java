package com.ibeifeng.sparkproject.dao;

import com.ibeifeng.sparkproject.domain.AdClickTrend;

/**
 * Created by Administrator on 2017/4/2.
 */
public interface IAdClickTrendDAO {
    void updateBatch(List<AdClickTrend> adClickTrends);
}
