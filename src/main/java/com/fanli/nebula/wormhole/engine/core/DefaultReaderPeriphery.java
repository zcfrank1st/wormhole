package com.fanli.nebula.wormhole.engine.core;

import com.fanli.nebula.wormhole.common.interfaces.IReaderPeriphery;
import com.fanli.nebula.wormhole.common.interfaces.ISourceCounter;
import com.fanli.nebula.wormhole.common.interfaces.ITargetCounter;
import com.fanli.nebula.wormhole.common.interfaces.IParam;

class DefaultReaderPeriphery implements IReaderPeriphery {

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		//do nothing
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter, int i) {
		//do nothing
	}

}
