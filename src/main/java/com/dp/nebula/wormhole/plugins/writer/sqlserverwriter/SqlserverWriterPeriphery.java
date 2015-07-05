package com.dp.nebula.wormhole.plugins.writer.sqlserverwriter;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;

/**
 * Created by zcfrank1st on 7/5/15.
 */
public class SqlserverWriterPeriphery implements IWriterPeriphery{
    public void rollback(IParam param) {

    }

    public void prepare(IParam param, ISourceCounter counter) {

    }

    public void doPost(IParam param, ITargetCounter counter, int faildSize) {

    }
}
