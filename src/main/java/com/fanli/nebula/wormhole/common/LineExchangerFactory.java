package com.fanli.nebula.wormhole.common;

import java.util.List;

import com.fanli.nebula.wormhole.common.interfaces.ILineReceiver;
import com.fanli.nebula.wormhole.common.interfaces.ILineSender;
import com.fanli.nebula.wormhole.engine.storage.IStorage;

public class LineExchangerFactory {
	
	public static ILineSender createNewLineSender(IStorage storageForRead, List<IStorage> storageForWrite){
	
		return new BufferedLineExchanger(storageForRead, storageForWrite);
	}
	
	public static ILineReceiver createNewLineReceiver(IStorage storageForRead, List<IStorage> storageForWrite){

		return new BufferedLineExchanger(storageForRead, storageForWrite);
	}

}
