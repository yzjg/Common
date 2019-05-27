package com.ibeifeng.sparkproject.dao;

/**
 * Created by Administrator on 2017/4/2.
 */
public interface IAdBlacklistDAO {
    void  insertBatch(List<AdBlacklist>  adBlacklists);
    List<AdBlacklist>  findAll();

}
