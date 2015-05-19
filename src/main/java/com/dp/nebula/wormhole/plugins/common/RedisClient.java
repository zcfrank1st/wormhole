package com.dp.nebula.wormhole.plugins.common;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.dianping.data.query.redis.RedisStoreKey;
import com.dianping.data.query.redis.RedisTable;
import com.dianping.data.query.redis.RedisWormhole;
import com.dianping.data.query.utils.RedisKeyUtil;

public final class RedisClient {
	private final static Logger LOG = Logger.getLogger(RedisClient.class);

	private static final int SLEEP_TIME = 5000;
	private static final int REPEAT_TIMES = 10;

	private int batchSize;

	private List<String> kList;
	private List<String> vList;
	private int expireTime;
	
	private RedisTable redisTable;
	
	// Redis service
	private static RedisWormhole redisWormhole = new RedisWormhole();;
	
	
	public RedisClient(int batchSize, String redisFamily) throws Exception{
		this.batchSize = batchSize; 
//		this.redisFamily = redisFamily;
		this.redisTable = redisWormhole.getRedisTable(redisFamily);
        LOG.info("tableInfo = " + redisTable);

        kList = new ArrayList<String>();
		vList = new ArrayList<String>();
	}

	public void setBatch(String key, String value, int expireTime) throws Exception {
		kList.add(key);
        vList.add(value);
        this.expireTime = expireTime;
        if (kList.size() >= batchSize) {
        	int i = 0;
			for(i = 0 ; i <= REPEAT_TIMES; i++){
				try {
                	redisWormhole.mset(redisTable, kList, vList, expireTime);
                	kList.clear();
                    vList.clear();
                    break;
                } catch (Exception e) {
                    LOG.error("", e);
                    Thread.sleep(SLEEP_TIME);
					if(i == REPEAT_TIMES){
						throw new Exception(e);
					}
                }
            }       
        }
    }

//	public void setBatch(String key, String value) throws IOException {
//		rkList.add(new RedisKey(redisFamily, key));
//		vList.add(value);
//		
//		if (rkList.size() >= batchSize) {
//			while(true){
//				try {
//					redisUse.mset(rkList,vList);
//					rkList.clear();
//					vList.clear();
//					break;
//				} catch (Exception e) {
//		            LOG.error(e.getMessage());
//				}
//			}		
//		}
//	}

    public void delete(String key) {
        try {
            redisWormhole.del(redisTable, key);
        } catch(Exception e) {
            LOG.error(e.getMessage());
        }
    }
    
    public void hset(String key, Map<String, String> hashValue, int expireTime){
        try {
            RedisStoreKey storeKey = RedisKeyUtil.getRedisKey(redisTable, key);
            redisWormhole.hset(redisTable, storeKey, null, null, hashValue, expireTime);
        } catch(Exception e) {
            LOG.error("", e);
        }
    }
    
    public void hdel(String key, List<String> columnList){
        try {
            int size = columnList.size();
            if(size == 0){
                return;
            }
            RedisStoreKey storeKey = RedisKeyUtil.getRedisKey(redisTable, key);
            String[] fields = new String[columnList.size()];
            for(int i = 0; i < size; i++){
                fields[i] = columnList.get(i);
            }
            redisWormhole.hdel(redisTable, storeKey, fields);
        } catch(Exception e) {
            LOG.error("", e);
        }
    }
    
    public void hdel(String key, String[] fields){
        try {
            RedisStoreKey storeKey = RedisKeyUtil.getRedisKey(redisTable, key);
            redisWormhole.hdel(redisTable, storeKey, fields);
        } catch(Exception e) {
            LOG.error("", e);
        }
    }
    
     

	public void close() throws Exception {
		
	}

	public void flush() throws Exception {
		if (kList.size() > 0) {
			int i = 0;
			for(i = 0 ; i <= REPEAT_TIMES; i++){
				try {
                	redisWormhole.mset(redisTable, kList, vList, expireTime);
					kList.clear();
					vList.clear();
					break;
				} catch (Exception e) {
                    LOG.error("", e);
					Thread.sleep(5000);
					if(i == REPEAT_TIMES){
						throw new Exception(e);
					}
				}
			}
		}
	}
}