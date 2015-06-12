package com.dp.nebula.wormhole.common.utils.confloader;

import com.dp.nebula.wormhole.common.DefaultParam;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.config.JobConf;
import com.dp.nebula.wormhole.common.config.JobPluginConf;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.utils.StringUtil;
import com.dp.nebula.wormhole.common.utils.TimeUtils;
import com.dp.nebula.wormhole.common.utils.confloader.constant.DateConst;
import com.google.gson.Gson;
import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbutils.QueryRunner;
import org.apache.commons.dbutils.handlers.ColumnListHandler;
import org.codehaus.jackson.JsonParser;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.type.TypeReference;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

/**
 * Created by zcfrank1st on 3/18/15.
 */
public class LoadJobConfFromOthers implements JobConfLoader{
    private static final Logger logger = LoggerFactory.getLogger(LoadJobConfFromOthers.class);

    private static final String driver = "com.mysql.jdbc.Driver";
    private static final String url = "jdbc:mysql://10.1.1.220:3306/DianPingDW?autoReconnect=true&amp;autoReconnectForPools=true&amp;useUnicode=true&amp;characterEncoding=utf-8S";
    private static final String user = "DianPingDW";
    private static final String password = "Diand32adjl3dvDW";
    private static final String sql = "select parameter_map from etl_load_cfg where task_id = ? order by type";

    private static final String WHERE_CLAUSE = "where";

    private static final String PARTITION_CONDITION = "hive_table_add_partition_condition";
    private static final String DIR = "dir";
    private static final String PREFIX_FILENAME = "prefix_filename";


    private static BasicDataSource dataSource = buildDataSource();

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
        List<Object> conf = null;
        try {
            conf = getConfFromMysql(taskId);
            List<String> transformedConf = replaceTimePattern(conf, Long.parseLong(time), offset);
            return JobConfGenerater(taskId, transformedConf);
        } catch (Exception e) {
            throw new WormholeException("load job conf failed");
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

    private List<Object> getConfFromMysql (String taskId) throws SQLException {
        QueryRunner qr = new QueryRunner(dataSource);
        List<Object> conf = qr.query(sql, new ColumnListHandler(1), taskId);
        for (Object o : conf) {
            logger.info((String) o + "\n");
        }
        return conf;
    }

    // for updating transport
    public JobConf loadJobConf4UpdatingTransport(String taskId, String time, String offset, String updateColumn) throws IOException {
        logger.info("taskid: " + taskId + " time: " + time + " offset: " + offset);
        List<Object> conf = null;
        DateTime dateTime = new DateTime(Long.parseLong(time) * 1000);
        Gson gson = new Gson();
        try {
            conf = getConfFromMysql(taskId);
            String conf0 = ((String)conf.get(0)).replace(":null", ":\"\""); // reader conf
            ObjectMapper mapper = new ObjectMapper();
            mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
            mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
            Map<String,String> readerMap = mapper.readValue(conf0, new TypeReference<HashMap<String,String>>(){});
            String sql = readerMap.get("sql");
            if (sql.toLowerCase().contains(WHERE_CLAUSE)) {
                sql += " and " + updateColumn + " >= '" + dateTime.minusDays(1).toString("YYYY-MM-dd") + "'";
            } else {
                sql += " where " + updateColumn + " >= '" + dateTime.minusDays(1).toString("YYYY-MM-dd") + "'";
            }
            logger.info("updating transport sql is " + sql);
            readerMap.put("sql", sql);
            String readerJson = gson.toJson(readerMap);
            conf.remove(0);
            conf.add(0, readerJson);
            logger.info("current json is :" + readerJson);

            // writer conf
            for (int i = 1; i <= conf.size() - 1; i ++) {
                Map<String,String> writerMap = mapper.readValue(((String)conf.get(i)).replace(":null", ":\"\""), new TypeReference<HashMap<String,String>>(){});
                String condition1 = writerMap.get(PARTITION_CONDITION);
                String condition2 = writerMap.get(DIR);
                String condition3 = writerMap.get(PREFIX_FILENAME);

                condition1 = condition1.trim() + "_delta";
                condition2 = condition2.replace(condition3.trim(), condition3.trim() + "_delta");
                condition3 = condition3.trim() + "_delta";

                writerMap.put(PARTITION_CONDITION, condition1);
                writerMap.put(DIR, condition2);
                writerMap.put(PREFIX_FILENAME, condition3);

                String writerJson = gson.toJson(writerMap);
                conf.remove(i);
                conf.add(i, writerJson);
            }
            List<String> transformedConf = replaceTimePattern(conf, Long.parseLong(time), offset);
            return JobConfGenerater(taskId, transformedConf);
        } catch (Exception e) {
            throw new WormholeException(e.getMessage());
        }
    }

    private JobConf JobConfGenerater (String taskId, List<String> confList) throws IOException {
        ObjectMapper mapper = new ObjectMapper();
        mapper.configure(JsonParser.Feature.ALLOW_UNQUOTED_CONTROL_CHARS, true);
        mapper.configure(JsonParser.Feature.ALLOW_BACKSLASH_ESCAPING_ANY_CHARACTER, true);
        JobConf jobConf = new JobConf();

        JobPluginConf readerJobConf = new JobPluginConf();
        List<JobPluginConf> writerJobConfs = new ArrayList<JobPluginConf>();

        Map<String,String> readerMap = mapper.readValue(confList.get(0), new TypeReference<HashMap<String,String>>(){});
        String readerName = readerMap.get("plugin");
        String readerId = readerMap.get("id");
        readerJobConf.setPluginName(readerName.trim().toLowerCase());
        readerJobConf.setId(readerId == null ? "reader-id-" + readerName
                : readerId.trim());
        IParam readerPluginParam = new DefaultParam(readerMap);
        readerJobConf.setPluginParam(readerPluginParam);

        int i = 1;
        while (i <= confList.size() - 1) {
            JobPluginConf writerJobConf = new JobPluginConf();
            Map<String,String> writerMap = mapper.readValue(confList.get(i), new TypeReference<HashMap<String,String>>(){});
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
    }
}
