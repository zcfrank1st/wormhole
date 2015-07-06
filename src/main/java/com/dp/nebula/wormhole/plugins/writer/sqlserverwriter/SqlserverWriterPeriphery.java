package com.dp.nebula.wormhole.plugins.writer.sqlserverwriter;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import com.dp.nebula.wormhole.plugins.common.DBSource;
import org.apache.commons.dbutils.QueryRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by zcfrank1st on 7/5/15.
 */
public class SqlserverWriterPeriphery implements IWriterPeriphery{
    private Logger logger = LoggerFactory.getLogger(SqlserverWriterPeriphery.class);

    private Connection conn;
    private String username;
    private String password;
    private String ip;
    private String port = "1433";
    private String dbname;
    private int concurrency;

    private void getConnection(IParam param) {
        this.username = param.getValue(ParamKey.username, "");
        this.password = param.getValue(ParamKey.password, "");
        this.ip = param.getValue(ParamKey.ip,"");
        this.port = param.getValue(ParamKey.port, this.port);
        this.dbname = param.getValue(ParamKey.dbname, "");
        this.concurrency = param.getIntValue(ParamKey.concurrency, 1);
        Properties p = createProperties(param.getValue(ParamKey.url, null));
        try {
            DBSource.register(SqlserverWriter.class, this.ip, this.port, this.dbname, p);
            conn = DBSource.getConnection(SqlserverWriter.class, ip, port, dbname);
        } catch (Exception e) {
            throw new WormholeException(e, JobStatus.WRITE_CONNECTION_FAILED.getStatus());
        }
    }

    private Properties createProperties(String url) {
        Properties p = new Properties();
        if(url == null) {
            url = String.format("jdbc:sqlserver://%s:%s;DatabaseName=%s", this.ip,this.port,this.dbname);
        }
        p.setProperty("driverClassName", "com.microsoft.sqlserver.jdbc.SQLServerDriver");
        p.setProperty("url", url);
        p.setProperty("username", username);
        p.setProperty("password", password);
        p.setProperty("maxActive", String.valueOf(concurrency + 2));
        p.setProperty("initialSize", String.valueOf(concurrency + 2));
        p.setProperty("maxIdle", "1");
        p.setProperty("maxWait", "1000");

        logger.debug(String.format("SqlserverWriter try connection: %s .", url));
        return p;
    }

    public void rollback(IParam param) {

    }

    public void prepare(IParam param, ISourceCounter counter) {
        getConnection(param);
        String preOperation = param.getValue(ParamKey.pre, "");
        logger.info("sqlserver writer pre operation : " + preOperation);
        try {
            QueryRunner qr = new QueryRunner();
            qr.update(conn, preOperation);
        } catch (SQLException e) {
            throw new WormholeException(e.getMessage());
        }
    }

    public void doPost(IParam param, ITargetCounter counter, int faildSize) {

    }
}
