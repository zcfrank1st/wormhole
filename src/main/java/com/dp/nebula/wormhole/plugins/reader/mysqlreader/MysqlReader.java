package com.dp.nebula.wormhole.plugins.reader.mysqlreader;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILineSender;
import com.dp.nebula.wormhole.common.interfaces.IReader;
import com.dp.nebula.wormhole.plugins.common.DBResultSetSender;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import com.dp.nebula.wormhole.plugins.common.DBUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.Map;

public class MysqlReader extends AbstractPlugin implements IReader{

	static final int PLUGIN_NO = 1;
	
	static final int ERROR_CODE_ADD = JobStatus.PLUGIN_BASE*PLUGIN_NO;
	
	private Connection conn;

	private String ip = "";

	private String port = "3306";

	private String jdbcRef = "";

	private String dbname;
	
	private String sql;
	
	private Log logger = LogFactory.getLog(MysqlReader.class);
	

	@Override
	public void init() {
		/* for database connection */
		this.ip = getParam().getValue(ParamKey.ip,"");
		this.port = getParam().getValue(ParamKey.port, this.port);
		//this.jdbcRef = getParam().getValue(ParamKey.jdbcRef, "");
		this.dbname = getParam().getValue(ParamKey.dbname,"");
		this.sql = getParam().getValue(ParamKey.sql, "").trim();
	}

	@Override
	public void connection() {
//		try {
//			conn = ZebraPool.INSTANCE.getPool(jdbcRef, ZebraPoolType.READ).getConnection();
//			logger.info("current connection :    " + conn);
//		} catch (SQLException e) {
//			throw new WormholeException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + ERROR_CODE_ADD);
//		}
		try {
			conn = DBSource.getConnection(this.getClass(), ip, port, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.READ_CONNECTION_FAILED.getStatus() + ERROR_CODE_ADD);
		}
	}

	@Override
	public void read(ILineSender lineSender){
		DBResultSetSender proxy = DBResultSetSender.newSender(lineSender);
		proxy.setMonitor(getMonitor());
		proxy.setDateFormatMap(genDateFormatMap());
		if(sql.isEmpty()){
			logger.error("Sql for mysqlReader is empty.");
			throw new WormholeException("Sql for mysqlReader is empty.",JobStatus.READ_FAILED.getStatus()+ERROR_CODE_ADD);
		}
		logger.info(String.format("MysqlReader start to query %s .", sql));
		for(String sqlItem:sql.split(";")){
			sqlItem = sqlItem.trim();
			if(sqlItem.isEmpty()) {
				continue;
			}
			logger.debug(sqlItem);
			ResultSet rs = null;
			try {
//				logger.info("current running sql: " + sql);
				rs = DBUtils.query(conn, sqlItem);
//				logger.info("query result : " + rs);
				proxy.sendToWriter(rs);
				proxy.flush();
			} catch (SQLException e) {
				logger.error("Mysql read failed",e);
				throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+ERROR_CODE_ADD);
			} catch (WormholeException e1) {
				e1.setStatusCode(e1.getStatusCode() + ERROR_CODE_ADD);
				throw e1;
			} finally {
				if (null != rs) {
				    try {
						DBUtils.closeResultSet(rs);
					} catch (SQLException e) {
						logger.error("MysqlReader close resultset error ");
						throw new WormholeException(e,JobStatus.READ_FAILED.getStatus()+ERROR_CODE_ADD);	
					}
	            }
			}
		}
	}

	@Override
	public void finish(){
		try {
			if (conn != null) {
				conn.close();
			}
			conn = null;
		} catch (SQLException e) {
			logger.error("Close connection failed",e);
		}
	}

	private Map<String, SimpleDateFormat> genDateFormatMap() {
		Map<String, SimpleDateFormat> mapDateFormat = new HashMap<String, SimpleDateFormat>();
		mapDateFormat.clear();
		mapDateFormat.put("datetime", new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss"));
		mapDateFormat.put("timestamp", new SimpleDateFormat(
				"yyyy-MM-dd HH:mm:ss"));
		mapDateFormat.put("time", new SimpleDateFormat("HH:mm:ss"));
		mapDateFormat.put("date", new SimpleDateFormat(
				"yyyy-MM-dd"));
		return mapDateFormat;
	}
}
