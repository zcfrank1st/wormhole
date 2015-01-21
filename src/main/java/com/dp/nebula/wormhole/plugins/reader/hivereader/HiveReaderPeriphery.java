package com.dp.nebula.wormhole.plugins.reader.hivereader;

import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.IReaderPeriphery;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.plugins.common.DFSUtils;
import com.google.common.base.Preconditions;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.joda.time.DateTime;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;

public class HiveReaderPeriphery implements IReaderPeriphery {
	private static final Logger LOG = Logger
			.getLogger(HiveReaderPeriphery.class);

	private static final String INSERT_SQL_PATTERN = "INSERT OVERWRITE DIRECTORY '%s' %s";

	private static final int EXEC_TIME_THREDHOLD = 50; // 设置任务最多执行时间

	private String path = "jdbc:hive://10.1.1.161:10000/default";
	private String username = "";
	private String password = "";
	private String sql = "";
	private String dataDir = "";
	private int reduceNumber = -1;
	public Path absolutePath;
	private String mode = HiveReaderMode.READ_FROM_HIVESERVER.getMode();
	private HiveJdbcClient client;
	private Configuration conf;
	private FileSystem fs;

	@Override
	public void prepare(IParam param, ISourceCounter counter) {
		mode = param.getValue(ParamKey.mode, mode);
		Preconditions
				.checkArgument(
						mode.equals(HiveReaderMode.READ_FROM_HDFS.getMode())
								|| mode.equals(HiveReaderMode.READ_FROM_HIVESERVER
										.getMode()) || mode.equals(HiveReaderMode.READ_FROM_LOCAL.getMode()),
						"hive reader mode should be READ_FROM_HDFS or READ_FROM_HIVESERVER or READ_FROM_LOCAL)");
		if (mode.equals(HiveReaderMode.READ_FROM_HDFS.getMode())) {
			path = param.getValue(ParamKey.path, path);
			username = param.getValue(ParamKey.username, username);
			password = param.getValue(ParamKey.password, password);
			reduceNumber = param.getIntValue(ParamKey.reduceNumber,
					reduceNumber);
			sql = param.getValue(ParamKey.sql, sql).trim();
			if (sql.endsWith(";")) {
				sql = StringUtils.substring(sql, 0, sql.length() - 1);
			}
			dataDir = param.getValue(ParamKey.dataDir, dataDir);
			try {
				createTempDir();
				param.putValue(ParamKey.dataDir, absolutePath.toString());
				sql = String.format(INSERT_SQL_PATTERN,
						absolutePath.toString(), sql);
				client = new HiveJdbcClient.Builder(path).username(username)
						.password(password).sql(sql).build();
				client.initialize();
				client.processInsertQuery(reduceNumber);
			} catch (Exception e) {
				throw new WormholeException(e,
						JobStatus.READ_FAILED.getStatus());
			} finally {
				if (client != null) {
					client.close();
				}
			}
		} else if (mode.equals(HiveReaderMode.READ_FROM_LOCAL.getMode())) {
			sql = param.getValue(ParamKey.sql, sql).trim();
			if (sql.endsWith(";")) {
				sql = StringUtils.substring(sql, 0, sql.length() - 1);
			}
			dataDir = param.getValue(ParamKey.dataDir, dataDir);
			try {
				createTempDir();
				param.putValue(ParamKey.dataDir, absolutePath.toString());
				sql = String.format(INSERT_SQL_PATTERN,
						absolutePath.toString(), sql);
				sql = sql.replaceAll("`","");
				runHiveCommandWithRetry();
			} catch (Exception e) {
				throw new WormholeException(e);
			}
		}
	}

	private void runHiveCommandWithRetry () throws IOException, InterruptedException {
		final List<String> command = new ArrayList<String>();
		command.add("hive");
		command.add("-e");
		command.add(sql);
		LOG.info("start hive -e => " + sql);
		final ProcessBuilder hiveProcessBuilder = new ProcessBuilder(command);
		hiveProcessBuilder.redirectErrorStream(true); // combine the stream
		final Process proc = hiveProcessBuilder.start();
		LOG.info("hive -e executing...");
		final DateTime startTime = new DateTime();
		Thread t = new Thread() {
			@Override
			public void run() {
				while(true) {
					try {
						if (proc.exitValue() == 0) {
							LOG.info("hive -e executed => SUCCESS");
							break;
						} else {
							throw new RuntimeException("hive execute => FAIL");
						}
					} catch (IllegalThreadStateException e) {
						try {
							LOG.info(new BufferedReader(new InputStreamReader(proc.getInputStream())).readLine());
						} catch (IOException e1) {
							LOG.error(e1.getMessage());
						}
						if (new DateTime().minusMinutes(EXEC_TIME_THREDHOLD).isAfter(startTime)) {
							try {
								proc.destroy();
								LOG.warn("first hive -e thread hung a long time, killed, retrying...");
								Process newProcess = hiveProcessBuilder.start();
								LOG.info("another hive -e thread start, executing...");
								if (newProcess.waitFor() == 0) {
									LOG.info("hive -e execute => SUCCESS");
									break;
								} else {
									LOG.error("hive -e execute => FAIL");
									throw new Exception("hive execute failed " +
											"\n" + "inputStream => " + newProcess.getInputStream());
								}
							} catch (IOException e1) {
								LOG.error(e.getMessage());
							} catch (InterruptedException e1) {
								LOG.error(e.getMessage());
							} catch (Exception e1) {
								LOG.error(e.getMessage());
							}
						}
					}
				}
			}
		};
		LOG.info("hive -e daemon thread starting...");
		t.start();
		LOG.info("hive -e daemon thread started => SUCCESS, hive -e still executing...");
		t.join();
	}

	private String createTempDir() throws Exception {
		conf = DFSUtils.getConf(dataDir, null);
		fs = DFSUtils.createFileSystem(new URI(dataDir), conf);
		absolutePath = new Path(dataDir, createFilename(sql));
		fs = absolutePath.getFileSystem(conf);
		if (fs.mkdirs(absolutePath)) {
			LOG.info("create data temp directory successfully "
					+ absolutePath.toString());
		} else {
			LOG.error("Failed to mkdir " + absolutePath.toString());
			throw new WormholeException(JobStatus.READ_FAILED.getStatus());
		}
		return absolutePath.toString();
	}

	private String createFilename(String sql) {
		return DigestUtils.md5Hex(sql + System.currentTimeMillis());
	}

	@Override
	public void doPost(IParam param, ITargetCounter counter) {
		if ((mode.equals(HiveReaderMode.READ_FROM_HDFS.getMode()) || mode.equals(HiveReaderMode.READ_FROM_LOCAL.getMode()))
				&& absolutePath != null) {
			try {
				if (fs.exists(absolutePath)) {
					fs.delete(absolutePath, true);
					LOG.info(absolutePath.toString()
							+ " has been deleted at dopost stage");
				}
			} catch (IOException e) {
				LOG.info("Failed to delete " + absolutePath.toString());
			}
		}
	}
}
