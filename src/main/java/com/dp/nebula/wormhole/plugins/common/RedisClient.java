package com.dp.nebula.wormhole.plugins.common;

import java.io.IOException;

import org.apache.log4j.Logger;

import com.dianping.data.query.redis.RedisTable;
import com.dianping.data.query.redis.RedisWormhole;

public final class RedisClient {
	private final static Logger LOG = Logger.getLogger(RedisClient.class);

//	private int batchSize;
//	private String redisFamily;
//	
//	private List<RedisKey> rkList;
//	private List<String> vList;
	private RedisTable redisTable;
	
	// Redis service
	private static RedisWormhole redisWormhole = null;
	
	static {
		try {
		    redisWormhole = new RedisWormhole();
			LOG.info("Get RedisUse success");
		} catch (Exception e) {
            LOG.error(e.getMessage());
		}
	}

	public RedisClient(int batchSize, String redisFamily) throws Exception{
//		this.batchSize = batchSize; 
//		this.redisFamily = redisFamily;
		this.redisTable = redisWormhole.getRedisTable(redisFamily);
        LOG.info("tableInfo = " + redisTable);

//		rkList = new ArrayList<RedisKey>();
//		vList = new ArrayList<String>();
	}

	public void set(String key, String value, int expireTime) {
        try {
            redisWormhole.set(redisTable, key, value, expireTime);
        } catch (Exception e) {
            LOG.error(e.getMessage());
        }
    }

//	public void setBatch(String key, String value, int expireTime) throws IOException {
//        rkList.add(new RedisKey(redisFamily, key));
//        vList.add(value);
//        
//        if (rkList.size() >= batchSize) {
//            while(true){
//                try {
//                    redisUse.mset(rkList, vList, expireTime);
//                    rkList.clear();
//                    vList.clear();
//                    break;
//                } catch (Exception e) {
//                    LOG.error(e.getMessage());
//                }
//            }       
//        }
//    }

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

	public void close() throws IOException {
		
	}

	public void flush() throws IOException {
	}
}