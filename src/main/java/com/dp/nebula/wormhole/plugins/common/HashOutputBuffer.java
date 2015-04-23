package com.dp.nebula.wormhole.plugins.common;

import java.util.HashMap;
import java.util.Map;

public class HashOutputBuffer implements OutputBufferInterface {
    private Map<String, String> hashOutput = new HashMap<String, String>();
    @Override
    public void put(String key, String value) {
        hashOutput.put(key, value);
    }
    
    public Map<String, String> getHashOutput(){
        return hashOutput;
    }
    
    public String toString(){
        return hashOutput.toString();
    }
    
    @Override
    public void clear() {
        hashOutput.clear();
        
    }

}
