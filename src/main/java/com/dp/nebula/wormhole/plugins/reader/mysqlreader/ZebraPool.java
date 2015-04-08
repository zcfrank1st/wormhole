package com.dp.nebula.wormhole.plugins.reader.mysqlreader;

import com.dianping.zebra.group.jdbc.GroupDataSource;
import com.dp.nebula.wormhole.common.WormholeException;

/**
 * Created by zcfrank1st on 4/8/15.
 */
public enum ZebraPool {
    INSTANCE;

    public GroupDataSource groupDataSource = null;

    public GroupDataSource getPool(String jdbcRef) {
        if (groupDataSource != null) {
            return groupDataSource;
        }
        synchronized (this) {
            if (!jdbcRef.equals("")) {
                groupDataSource = new GroupDataSource(jdbcRef);
                groupDataSource.setMaxPoolSize(4);
                groupDataSource.init();
                return groupDataSource;
            }
        }
        throw new WormholeException("get zebra pool error");
    }
}
