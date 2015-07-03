package com.fanli.nebula.wormhole.common.interfaces;

public interface ITransformer {
	
	ILine transform(ILine line);
	
	ILine transform(ILine line, String params);

}

