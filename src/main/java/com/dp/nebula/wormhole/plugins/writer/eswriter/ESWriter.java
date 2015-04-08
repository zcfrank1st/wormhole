package com.dp.nebula.wormhole.plugins.writer.eswriter;

import com.dp.nebula.wormhole.common.AbstractPlugin;
import com.dp.nebula.wormhole.common.interfaces.ILine;
import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
import com.dp.nebula.wormhole.common.interfaces.IWriter;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.RootLogger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.action.bulk.BulkRequestBuilder;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.indices.IndexMissingException;
import org.elasticsearch.node.Node;

import java.io.IOException;

import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * Created by tianming.mao on 15/2/28.
 */
public class ESWriter extends AbstractPlugin implements IWriter {

    private final static Logger LOG = Logger.getLogger(ESWriter.class);

    private Node node = null;
    private Client client = null;
    private String index = null;

    private String clusterName = null;
    private String transportAddress = null;
    private String indexPrefix = null;
    private String indexDate = null;
    private String type = null;
    private int bulkSize = -1;
    private String[] fieldList = null;

    @Override
    public void init(){
        LOG.info("%% init %%");

        clusterName = getParam().getValue(ParamKey.clusterName);
        transportAddress = getParam().getValue(ParamKey.transportAddress);
        indexPrefix = getParam().getValue(ParamKey.indexPrefix);
        indexDate = getParam().getValue(ParamKey.indexDate);
        type = getParam().getValue(ParamKey.type);
        bulkSize = getParam().getIntValue(ParamKey.bulkSize);

        index = indexPrefix + "." + indexDate;

        String fields = getParam().getValue(ParamKey.fields);
        if (fields == null) throw new AssertionError();
        LOG.info("fields: " + fields);

        fieldList = fields.split(",");
        StringBuilder sb = new StringBuilder().append("field list: \n");
        for (int i = 0; i < fieldList.length; i++) {
            fieldList[i] = fieldList[i].trim();
            sb.append("%%").append(fieldList[i]).append("%%\n");
        }
        LOG.info(sb.toString());
    }

    @Override
    public void connection() {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        client = new org.elasticsearch.client.transport.TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(transportAddress, 9300));
    }

    @Override
    public void write(ILineReceiver lineReceiver) {
        LOG.info("%% write %%");
        ILine line = null;
        BulkRequestBuilder bulkRequest = client.prepareBulk();
        int linesInBulkRequest = 0;

        while((line = lineReceiver.receive()) != null) {
            if (line.getFieldNum() != fieldList.length) throw new AssertionError();

            try {
                XContentBuilder jb = jsonBuilder();
                jb.startObject();
                for (int i = 0; i < fieldList.length; i++) {
                    jb.field(fieldList[i], line.getField(i));
                }
                jb.endObject();

                bulkRequest.add(client.prepareIndex(index, type).setSource(jb));
                linesInBulkRequest++;

                if (linesInBulkRequest == bulkSize) {
                    triggerBulkRequest(bulkRequest);
                    linesInBulkRequest = 0;
                    bulkRequest = client.prepareBulk();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }

        if (linesInBulkRequest != 0) {
            triggerBulkRequest(bulkRequest);
        }
    }

    private void triggerBulkRequest(BulkRequestBuilder bulkRequest) {
        BulkResponse bulkResponse = bulkRequest.execute().actionGet();

        if (bulkResponse.hasFailures()) {
            LOG.warn("bulk request has errors");
        }

        getMonitor().increaseSuccessLine(bulkRequest.numberOfActions());
        LOG.info("docs imported: " + bulkRequest.numberOfActions());
    }

    @Override
    public void commit() {
        LOG.info("%% commit %%");
    }

    @Override
    public void finish(){
        LOG.info("%% finish %%");
        client.close();
    }
}