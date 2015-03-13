package com.dp.nebula.wormhole.plugins.writer.eswriter;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;

/**
 * Created by litora on 15/2/28.
 */
public class ESWriterPeriphery implements IWriterPeriphery {
    @Override
    public void rollback(IParam param) {

    }

    @Override
    public void prepare(IParam param, ISourceCounter counter) {

    }

    @Override
    public void doPost(IParam param, ITargetCounter counter, int faildSize) {

    }


}