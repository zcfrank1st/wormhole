package com.dp.nebula.wormhole.common.utils.confloader;

import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.config.JobConf;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.utils.StringUtil;
import com.dp.nebula.wormhole.common.utils.TimeUtils;
import com.dp.nebula.wormhole.common.utils.confloader.constant.DateConst;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by zcfrank1st on 3/18/15.
 */
public class LoadJobConfFromMysql implements JobConfLoader{
    private static final Logger logger = LoggerFactory.getLogger(LoadJobConfFromMysql.class);

    public static final String driver = "com.mysql.jdbc.Driver";
    public static final String url = "jdbc:mysql://10.1.1.220:3306/DianPingDW?autoReconnect=true&amp;autoReconnectForPools=true&amp;useUnicode=true&amp;characterEncoding=utf-8S";
    public static final String user = "DianPingDW";
    public static final String password = "Diand32adjl3dvDW";
    public static final String sql = "select parameter_map from etl_load_cfg where task_id = ? order by type";

    public static BasicDataSource dataSource = buildDataSource();

    private static BasicDataSource buildDataSource() {
        BasicDataSource ds = new BasicDataSource();
        ds.setDriverClassName(driver);
        ds.setUrl(url);
        ds.setUsername(user);
        ds.setPassword(password);
        return ds;
    }


    private Connection connection = null;

    @Override
    public boolean initLoader() {
        try {
            connection = dataSource.getConnection();
            return connection != null;
        } catch (SQLException e) {
            return false;
        }
    }

    @Override
    public JobConf loadJobConf(String... args) { // taskId  time  offset
        String taskId = args[0];
        String time = args[1];
        String offset = args[2];
        logger.info("taskid: " + taskId + " time: " + time + " offset: " + offset);
        QueryRunner qr = new QueryRunner(dataSource);
        try {
            JobConf jobConf = new JobConf();
            JobPluginConf readerJobConf = new JobPluginConf();
            List<JobPluginConf> writerJobConfs = new ArrayList<JobPluginConf>();

            ObjectMapper mapper = new ObjectMapper();

            List<Object> conf = qr.query(sql, new ColumnListHandler(1), taskId);
            for (Object o : conf) {
                logger.info((String) o + "\n");
            }

            List<String> transformedConf = replaceTimePattern(conf, Long.parseLong(time), offset);

            Map<String,String> readerMap = mapper.readValue(transformedConf.get(0), new TypeReference<HashMap<String,String>>(){});
            String readerName = readerMap.get("plugin");
            String readerId = readerMap.get("id");
            readerJobConf.setPluginName(readerName.trim().toLowerCase());
            readerJobConf.setId(readerId == null ? "reader-id-" + readerName
                    : readerId.trim());
            IParam readerPluginParam = new DefaultParam(readerMap);
            readerJobConf.setPluginParam(readerPluginParam);

            int i = 1;
            while (i <= transformedConf.size() - 1) {
                JobPluginConf writerJobConf = new JobPluginConf();
                Map<String,String> writerMap = mapper.readValue(transformedConf.get(i), new TypeReference<HashMap<String,String>>(){});
                String writerName = writerMap.get("plugin");
                String writerId = writerMap.get("id");
                writerJobConf.setPluginName(writerName.trim().toLowerCase());
                writerJobConf.setId(writerId == null ? "writer-id-" + writerName
                        : writerId.trim());
                IParam writerPluginParam = new DefaultParam(writerMap);
                writerJobConf.setPluginParam(writerPluginParam);
                writerJobConfs.add(writerJobConf);
                i++;
            }

            jobConf.setId(taskId);
            jobConf.setReaderConf(readerJobConf);
            jobConf.setWriterConfs(writerJobConfs);

            return jobConf;
        } catch (Exception e) {
            throw new WormholeException(e.getMessage());
        }
    }


    private List<String> replaceTimePattern (List<Object> origin, long t, String offset) {
        Map<String,String> dateVars = new HashMap<String, String>();
        for(DateConst.BATCH_CAL_VARS var :DateConst.BATCH_CAL_VARS.values()){
            String key = var.toString();
            String dateVar = TimeUtils.transferVariable(key, t * 1000, offset);
            dateVars.put(key, dateVar);
        }
        List<String> transferResults = new ArrayList<String>();
        for (Object c : origin) {
            String content = StringUtil.replaceVariables((String)c, dateVars);
            transferResults.add(content);
        }
        return transferResults;
    }
}
