package com.dp.nebula.wormhole.plugins.common;

import com.dianping.zebra.group.jdbc.GroupDataSource;
import com.dp.nebula.wormhole.common.WormholeException;

/**
 * Created by zcfrank1st on 4/8/15.
 */
public enum ZebraPool {
    INSTANCE;

    public GroupDataSource groupDataSource = null;

    public GroupDataSource getPool(String jdbcRef, ZebraPoolType type) {
        if (groupDataSource != null) {
            return groupDataSource;
        }
        synchronized (this) {
            if (groupDataSource != null) {
                return groupDataSource;
            }
            if (!jdbcRef.equals("")) {
                groupDataSource = new GroupDataSource(jdbcRef);
                if (type == ZebraPoolType.READ) {
                    groupDataSource.setRouterType("load-balance");
                }
                groupDataSource.setInitialPoolSize(20);
                groupDataSource.setMaxPoolSize(40);
                groupDataSource.setExtraJdbcUrlParams("zeroDateTimeBehavior=convertToNull");
                groupDataSource.init();
                return groupDataSource;
            }
        }
        throw new WormholeException("get zebra pool error");
    }
}
