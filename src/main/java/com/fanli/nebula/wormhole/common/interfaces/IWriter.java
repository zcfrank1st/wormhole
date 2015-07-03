package com.fanli.nebula.wormhole.common.interfaces;

public interface IWriter extends IPlugin{
	
	void write(ILineReceiver lineReceiver);
	
	void commit();
	
}
