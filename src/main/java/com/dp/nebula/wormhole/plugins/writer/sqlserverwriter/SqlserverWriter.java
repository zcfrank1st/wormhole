package com.dp.nebula.wormhole.plugins.writer.sqlserverwriter;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * Created by zcfrank1st on 7/5/15.
 */
public class SqlserverWriter extends AbstractPlugin implements IWriter {
	private Connection conn;
	private String ip;
	private String dbname;
	private String port;
	private String username;
	private String password;
	private String tableName;
	private String columns;
	private String encoding;

	private static final int MAX_LINE = 1000;

	private Logger logger = LoggerFactory.getLogger(SqlserverWriter.class);

	@Override
	public void init() {
		this.ip = getParam().getValue(ParamKey.ip,"");
		this.dbname = getParam().getValue(ParamKey.dbname,"");
		this.port = getParam().getValue(ParamKey.port,"");
		this.username = getParam().getValue(ParamKey.username,"");
		this.password = getParam().getValue(ParamKey.password,"");
		this.tableName = getParam().getValue(ParamKey.tableName,"");
		this.columns = getParam().getValue(ParamKey.columns, "");
		this.encoding = getParam().getValue(ParamKey.encoding, "UTF8").toLowerCase();
	}

	@Override
	public void connection() {
		try {
			conn = DBSource.getConnection(SqlserverWriter.class, ip, port, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.WRITE_CONNECTION_FAILED.getStatus());
		}
	}

	@Override
	public void write(ILineReceiver receiver) {
		String runningSQL = buildWriteSql(receiver);
//		logger.info("Insert SQL - " + runningSQL + "\n");
		try {
			QueryRunner qr = new QueryRunner();
			String[] sqls = runningSQL.split("##");
			for (String sql: sqls) {
				qr.update(conn, sql);
			}
		} catch (SQLException e) {
			throw new WormholeException(e.getMessage());
		}
	}

	@Override
	public void commit() {
	}

	private String buildWriteSql (ILineReceiver receiver) {
		ILine line;
		String lineInit = "( ";
		String lineEnd = " )";
		String lineCollections = "";
		while ((line = receiver.receive()) != null) {
			int len = line.getFieldNum();
			String valueLine = "";
			for (int i = 0; i < len; i++) {
				if (i == len - 1) {
					if (line.getField(i) == null) {
						valueLine = valueLine  + line.getField(i)  + lineEnd;
					} else {
						valueLine = valueLine + "'" + line.getField(i) + "'" + lineEnd;
					}
					break;
				}
				if (i == 0) {
					if (line.getField(i) == null) {
						valueLine = lineInit  + line.getField(i)  + ", ";
					} else {
						valueLine = lineInit + "'" + line.getField(i) + "', ";
					}
				} else {
					if (line.getField(i) == null) {
						valueLine = valueLine  + line.getField(i)  + ", ";
					} else {
						valueLine = valueLine + "'" + line.getField(i) + "', ";
					}
				}
			}
			lineCollections = lineCollections + valueLine + "|";
			getMonitor().increaseSuccessLines();
		}
		String totalValues = lineCollections.substring(0, lineCollections.length() - 1);
		String[] totalSegments = totalValues.split("\\|");
		int segmentCount = totalSegments.length;

		// 分组
		int group = (int)Math.ceil((double) segmentCount / MAX_LINE);
//		logger.info("group number is: "+ group);
		int currentEle = 0;
		String multiSql = "";

		if ( group > 1) {
			for (int i = 1; i <= group; i++) {
				String groupValues = "";
				for(int j = 0; j < MAX_LINE;) {
					groupValues += totalSegments[currentEle] + ", ";
					if (currentEle == segmentCount -1) {
						multiSql += "INSERT INTO " + tableName + " (" + columns + ") " + "values " + groupValues.substring(0, groupValues.length() - 2);
						break;
					}
					j ++;
					currentEle ++;
					if (j == MAX_LINE) {
						multiSql += "INSERT INTO " + tableName + " (" + columns + ") " + "values " + groupValues.substring(0, groupValues.length() - 2) + "##";
						break;
					}
				}
			}
			return multiSql;
		} else {
			return "INSERT INTO " + tableName + " (" + columns + ") " + "values " + totalValues.replace("|", ",");
		}
	}

}
