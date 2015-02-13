package com.dp.nebula.wormhole.plugins.writer.hbasewriter;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.JobStatus;
import com.dp.nebula.wormhole.common.WormholeException;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import com.dp.nebula.wormhole.plugins.common.HBaseClient;
import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.UnsupportedEncodingException;

public class HBaseWriter extends AbstractPlugin implements IWriter {
    private final static Logger LOG = Logger.getLogger(HBaseWriter.class);

    private final static String DEFAULT_ENCODING = "UTF-8";
    private final static int MININUM_FIELD_NUM = 2;

    private int rowKeyIndex;
    private String columnsName;
    private byte[][] columnFamilies;
    private byte[][] qualifiers;
    private HBaseClient client;
    private int num_to_wait;
    private int wait_time;
    private boolean write_sleep;
    private boolean ignoreNullColumn;

    @Override
    public void init() {
        rowKeyIndex = getParam().getIntValue(ParamKey.rowKeyIndex, 0);
        columnsName = getParam().getValue(ParamKey.columnsName);
        num_to_wait = getParam().getIntValue(ParamKey.num_to_wait);
        wait_time   = getParam().getIntValue(ParamKey.wait_time);
        write_sleep = getParam().getBooleanValue(ParamKey.write_sleep);
        ignoreNullColumn = getParam().getBooleanValue(ParamKey.ignoreNullColumn, true);

        parseColumnsMapping(columnsName);
    }

    private void parseColumnsMapping(String columnsName) {
        if (StringUtils.isBlank(columnsName)) {
            throw new IllegalArgumentException("columns names can not be empty");
        }
        String[] columnsNameArray = StringUtils.split(columnsName, ',');
        columnFamilies = new byte[columnsNameArray.length][];
        qualifiers = new byte[columnsNameArray.length][];

        for (int i = 0; i < columnsNameArray.length; i++) {
            String[] parts = StringUtils.split(columnsNameArray[i], ':');
            if (!(parts.length == 2)) {
                throw new IllegalArgumentException(String.format(
                        "column name %s must be specified as cf:qualifier",
                        columnsNameArray[i]));
            }
            try {
                columnFamilies[i] = parts[0].getBytes(
                        DEFAULT_ENCODING);
                qualifiers[i] = parts[1].getBytes(
                        DEFAULT_ENCODING);
            } catch (UnsupportedEncodingException e) {
                throw new WormholeException(e,
                        JobStatus.READ_FAILED.getStatus());
            }
        }
    }

    @Override
    public void connection() {
        client = HBaseClient.getInstance();
        Preconditions.checkNotNull(client);
    }

    @Override
    public void write(ILineReceiver lineReceiver) {
        ILine line;
        int count = 0;
        try {
            while ((line = lineReceiver.receive()) != null) {
                int fieldNum = line.getFieldNum();
                if (fieldNum < MININUM_FIELD_NUM) {
                    LOG.warn("field number is less than " + MININUM_FIELD_NUM + " consider it as an empty line:" + line.toString(','));
                    continue;
                }
                String rowKey = line.getField(rowKeyIndex);
                if (StringUtils.isEmpty(rowKey)) {
                    LOG.warn("row key is null, ignore it");
                    getMonitor().increaseSuccessLines();
                    continue;
                }

                client.setRowKey(rowKey.getBytes(DEFAULT_ENCODING));

                for (int i = 0; i < rowKeyIndex; i++) {
                    if (line.getField(i) == null) {
                        if(!ignoreNullColumn){
                            client.addColumn(columnFamilies[i], qualifiers[i], null);
                        }
                        continue;
                    }
                    client.addColumn(columnFamilies[i], qualifiers[i], line
                            .getField(i).getBytes(DEFAULT_ENCODING));
                }
                for (int i = rowKeyIndex + 1; i < fieldNum; i++) {
                    if (line.getField(i) == null) {
                        if(!ignoreNullColumn){
                            client.addColumn(columnFamilies[i], qualifiers[i], null);
                        }
                        continue;
                    }
                    client.addColumn(columnFamilies[i-1], qualifiers[i-1], line
                            .getField(i).getBytes(DEFAULT_ENCODING));
                }

                client.insert();
                count ++;
                // for slow the hbase write speed
                if ( write_sleep ){
                    if ( count >= num_to_wait ){
                        Thread.sleep(wait_time);
                        count = 0;
                    }
                }
                getMonitor().increaseSuccessLines();
            }
        } catch (Exception e) {
            throw new WormholeException(e, JobStatus.WRITE_DATA_EXCEPTION.getStatus());
        }
    }

    @Override
    public void commit() {
        try {
            client.flush();
        } catch (IOException e) {
            throw new WormholeException(e, JobStatus.WRITE_DATA_EXCEPTION.getStatus());
        }
    }

    @Override
    public void finish() {
        try {
            client.close();
        } catch (IOException e) {
            throw new WormholeException(e, JobStatus.WRITE_DATA_EXCEPTION.getStatus());
        }
    }
}
