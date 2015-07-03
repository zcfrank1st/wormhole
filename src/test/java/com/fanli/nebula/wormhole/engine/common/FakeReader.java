package com.fanli.nebula.wormhole.engine.common;

import java.util.Map;

import com.fanli.nebula.wormhole.common.interfaces.IReader;
import com.fanli.nebula.wormhole.common.AbstractPlugin;
import com.fanli.nebula.wormhole.common.interfaces.ILineSender;

public class FakeReader extends AbstractPlugin implements IReader {

	@Override
	public void init() {}

	@Override
	public void connection() {}

	@Override
	public void finish() {}

	@Override
	public Map<String, String> getMonitorInfo() {
		return null;
	}

	@Override
	public void read(ILineSender lineSender) {}

}
