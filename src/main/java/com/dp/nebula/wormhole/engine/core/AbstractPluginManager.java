package com.dp.nebula.wormhole.engine.core;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.utils.Environment;
import com.dp.nebula.wormhole.engine.config.PluginConfParamKey;
import com.dp.nebula.wormhole.plugins.common.ParamKey;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.File;
import java.io.FileInputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

abstract class AbstractPluginManager {
	
	private static final Log s_logger = LogFactory.getLog(AbstractPluginManager.class);
	private static final String PARAM_KEY_CURRENCY = "concurrency";
	//private static final String WORMHOLE_CONNECT_FILE = "WORMHOLE_CONNECT_FILE";
	//private static final String LION_PROJECT = "LION_PROJECT";


	private static final String CONNECT_PRIFIX = "mysql_";
	private static final String DAL_SPECIAL_DBS = "special_dbs";
	private static final String SPECIAL_SUFFIX = ".sdw";
	private static final String DAL_SPECIAL_DBS_LION_SEP = ";";


    /**
	 * get number of reader/writer threads running concurrently
	 * it's determined by parameter concurrency set in job.xml
	 * the valid value is between 1 to MAX_THREAD_NUMBER (it's set per plugin in plugin.xml)
	 * 
	 * @param jobParams
	 * @param pluginParams
	 * @return int 
	 */
	protected int getConcurrency(IParam jobParams, IParam pluginParams){
		int concurrency = jobParams.getIntValue(PARAM_KEY_CURRENCY, 1);
		int maxThreadNum = pluginParams.getIntValue(PluginConfParamKey.MAX_THREAD_NUMBER);
		if(concurrency <=0 || concurrency > maxThreadNum){
			s_logger.info("concurrency in conf:" + concurrency + " is invalid!");
			concurrency = 1;
		}
		
		return concurrency;
	}
	
	/**
	 * create thread pool to run reader/writer threads
	 * 
	 * @param concurrency
	 * @return ExecutorService
	 */
	protected ExecutorService createThreadPool(int concurrency){
		ThreadPoolExecutor tp = new ThreadPoolExecutor(concurrency, concurrency, 1L,TimeUnit.SECONDS,
				new LinkedBlockingQueue<Runnable>());
		tp.prestartCoreThread();
		return tp;
	}
	
	/**
	 * Decide whether the reader or writer thread is executed successfully
	 * if there is NO thread failed, it returns true; even if some of the thread is running
	 * 
	 * @param threadResultList
	 * @return boolean
	 */
	protected int getStatus(List<Future<Integer>> threadResultList, ExecutorService threadPool){
		for(Future<Integer> r: threadResultList){
			try {
				Integer result = r.get(1, TimeUnit.MICROSECONDS);
				if(result == null || result != JobStatus.SUCCESS.getStatus()){
					if(threadPool != null){
					//if one thread failed, stop all other threads in the thread pool
						threadPool.shutdownNow();
					}
					return result;
				}
			}catch(TimeoutException e){
				s_logger.debug("thread is not finished yet");
				continue;
			}catch (InterruptedException e) {
				s_logger.error("Interrupted Exception occurs when getting thread result!");
				continue;
			} catch (ExecutionException e) {
				threadPool.shutdownNow();
				s_logger.error("Execution Exception occurs when getting thread result, this should never happen!", e);
				return JobStatus.FAILED.getStatus();
			}
		}
		return JobStatus.SUCCESS.getStatus();
	}
	
	public boolean isSuccess(List<Future<Integer>> threadResultList, ExecutorService threadPool) {
		return getStatus(threadResultList,threadPool)==JobStatus.SUCCESS.getStatus();
	}
	
	public static void regDataSourceProp(IParam param) {
		File fileName = new File(Environment.CONNECTION_FILE);
		// String fileName = System.getenv(WORMHOLE_CONNECT_FILE);
		String connectProps = param.getValue(ParamKey.connectProps, null);
		if (connectProps != null) {
			Properties props = new Properties();
			try {
				props.load(new FileInputStream(fileName));
				param.putValue(ParamKey.ip, props.getProperty(connectProps + "." + ParamKey.ip).trim());
				param.putValue(ParamKey.port, props.getProperty(connectProps + "." + ParamKey.port).trim());
				param.putValue(ParamKey.username, props.getProperty(connectProps + "." + ParamKey.username).trim());
				param.putValue(ParamKey.password, props.getProperty(connectProps + "." + ParamKey.password).trim());
//				param.putValue(ParamKey.dbname, props.getProperty(connectProps + "." + ParamKey.dbname).trim());
			} catch (Exception e) {
				s_logger.error(e.getMessage(),e);
				throw new WormholeException(e,JobStatus.CONF_FAILED.getStatus());
			}
		}

		//zk
//        ConfigCache configCache = null;
//        String lionProject = System.getenv(LION_PROJECT);
//        String connectProps = StringUtils.lowerCase(param.getValue(ParamKey.connectProps,null));
//        s_logger.info("connectProp :\t"+connectProps);
//        if(connectProps != null && lionProject != null){
//            try{
//                configCache = ConfigCache.getInstance(EnvZooKeeperConfig.getZKAddress());
//				if (connectProps.startsWith(CONNECT_PRIFIX)) {
//					String realDb = connectProps.substring(6).trim(); // 去除'mysql_'前缀
//					String specialDbs = configCache.getProperty(lionProject + "." + DAL_SPECIAL_DBS);
//					for (String db : specialDbs.split(DAL_SPECIAL_DBS_LION_SEP)) {
//						if (db.trim().equals(realDb)) {
//							param.putValue(ParamKey.jdbcRef, realDb + SPECIAL_SUFFIX);
//						}
//					}
//					if (param.getValue(ParamKey.jdbcRef, null) == null)
//						param.putValue(ParamKey.jdbcRef, realDb);
//
//					s_logger.info("real jdbc ref is:  " + param.getValue(ParamKey.jdbcRef));
//				} else {
//					param.putValue(ParamKey.ip, configCache.getProperty(lionProject + "." + connectProps + "." + ParamKey.ip));
//					param.putValue(ParamKey.port, configCache.getProperty(lionProject + "." + connectProps + "." + ParamKey.port));
//					param.putValue(ParamKey.username, configCache.getProperty(lionProject + "." + connectProps + "." + ParamKey.username));
//					param.putValue(ParamKey.password, configCache.getProperty(lionProject + "." + connectProps + "." + ParamKey.password));
//					param.putValue(ParamKey.dbname, configCache.getProperty(lionProject + "." + connectProps + "." + ParamKey.dbname));
//				}
//            }catch (Exception e){
//                s_logger.error("read connect configuration from lion failure",e);
//                throw new WormholeException(e,JobStatus.CONF_FAILED.getStatus());
//            }
//        }
	}
 }
