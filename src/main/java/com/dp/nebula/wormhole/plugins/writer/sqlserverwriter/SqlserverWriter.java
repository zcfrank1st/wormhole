package com.dp.nebula.wormhole.plugins.writer.sqlserverwriter;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import org.apache.log4j.Logger;

import java.sql.Connection;

/**
 * Created by zcfrank1st on 7/5/15.
 */
public class SqlserverWriter extends AbstractPlugin implements IWriter {
	private Connection conn;

	private String ip = "";

	private String dbname = null;

	private String sql;

	private String table;

	private String columns;

	private String encoding = "UTF8";

    private ILine line = null;

	private String writerID;

	private Logger logger = Logger.getLogger(SqlserverWriter.class);

	@Override
	public void init() {
		this.ip = getParam().getValue(ParamKey.ip,"");
		this.dbname = getParam().getValue(ParamKey.dbname,"");
		this.table = getParam().getValue(ParamKey.tableName,"");
		this.columns = getParam().getValue(ParamKey.columns,"");
		this.encoding = getParam().getValue(ParamKey.encoding, "UTF8").toLowerCase();
		this.writerID	 = getParam().getValue(AbstractPlugin.PLUGINID, "");
	}

	@Override
	public void connection() {
		try {
			conn = DBSource.getConnection(SqlserverWriter.class, ip, writerID, dbname);
		} catch (Exception e) {
			throw new WormholeException(e, JobStatus.WRITE_CONNECTION_FAILED.getStatus());
		}
	}

	@Override
	public void write(ILineReceiver receiver) {
		logger.info(writerID + ": insert SQL - " + sql);
		// TODO 写入数据
	}

	@Override
	public void commit() {
	}

}
