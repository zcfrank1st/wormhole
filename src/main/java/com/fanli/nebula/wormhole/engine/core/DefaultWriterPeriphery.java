package com.fanli.nebula.wormhole.engine.core;

import com.fanli.nebula.wormhole.common.interfaces.ITargetCounter;
import com.fanli.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.fanli.nebula.wormhole.common.interfaces.IParam;
import com.fanli.nebula.wormhole.common.interfaces.ISourceCounter;

class DefaultWriterPeriphery implements IWriterPeriphery {

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		// do nothing
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter, int i) {
		// do nothing
		
	}

	@Override
	public void rollback(IParam param) {
		// do nothing
	}

}
