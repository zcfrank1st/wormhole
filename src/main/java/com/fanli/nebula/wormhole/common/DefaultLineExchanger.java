package com.fanli.nebula.wormhole.common;

import com.fanli.nebula.wormhole.common.interfaces.ILine;
import com.fanli.nebula.wormhole.common.interfaces.ILineReceiver;
import com.fanli.nebula.wormhole.common.interfaces.ILineSender;
import com.fanli.nebula.wormhole.engine.storage.IStorage;

public class DefaultLineExchanger implements ILineSender, ILineReceiver{

	private IStorage storage;

	public DefaultLineExchanger(IStorage storage) {
		super();
		this.storage = storage;
	}

	@Override
	public ILine createNewLine() {
		return new DefaultLine();
	}

	@Override
	public Boolean send(ILine line) {
		return storage.push(line);
	}

	@Override
	public void flush() {		
	}

	@Override
	public ILine receive() {
        return storage.pull();
	}

}
