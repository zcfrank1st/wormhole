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
		logger.info("Insert SQL - " + runningSQL + "\n");
		try {
			QueryRunner qr = new QueryRunner();
			qr.update(conn, runningSQL);
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
					valueLine = valueLine + "'" + line.getField(i) + "'" + lineEnd;
					break;
				}
				if (i == 0) {
					valueLine = lineInit + "'" + line.getField(i) + "', ";
				} else {
					valueLine = valueLine + "'" + line.getField(i) + "', ";
				}
			}
			lineCollections = lineCollections + valueLine + ", ";
			getMonitor().increaseSuccessLines();
		}

		return "INSERT INTO " + tableName + " (" + columns + ") " + "values " + lineCollections.substring(0, lineCollections.length() - 2);
	}

}
