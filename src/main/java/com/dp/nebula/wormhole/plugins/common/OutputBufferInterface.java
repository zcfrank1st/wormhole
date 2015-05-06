package com.dp.nebula.wormhole.plugins.common;

import java.util.Map;

public interface OutputBufferInterface {

	public void put(String key, String value);
	
	public String toString();
	
	public Map<String, String> getHashOutput();
		
	public void clear();
}
