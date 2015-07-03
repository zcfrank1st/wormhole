package com.fanli.nebula.wormhole.plugins.common;

import java.util.Map;

public class BufferString implements OutputBufferInterface {

	private String buffer;
	private String separator;
	private boolean isBegin; 
	
	public BufferString(String separator){
		this.separator = new String(separator);
		this.buffer = new String("");
		isBegin = true;
	}
	
	@Override
	public void put(String key, String value) {
		if(!isBegin)
		{
			buffer += separator;
		}
		buffer += value;
		isBegin = false;
	}
	
	public String toString(){
		return buffer;
	}

	@Override
	public void clear() {
		buffer = "";
		isBegin = true;
	}
	
    @Override
    public Map<String, String> getHashOutput() {
        return null;
    }

}
