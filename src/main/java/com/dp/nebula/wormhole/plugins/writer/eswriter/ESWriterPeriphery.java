package com.dp.nebula.wormhole.plugins.writer.eswriter;

import com.dp.nebula.wormhole.common.interfaces.IParam;
import com.dp.nebula.wormhole.common.interfaces.ISourceCounter;
import com.dp.nebula.wormhole.common.interfaces.ITargetCounter;
import com.dp.nebula.wormhole.common.interfaces.IWriterPeriphery;
import org.apache.log4j.Logger;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequestBuilder;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexResponse;
import org.elasticsearch.action.admin.indices.optimize.OptimizeRequestBuilder;
import org.elasticsearch.action.admin.indices.optimize.OptimizeResponse;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsRequestBuilder;
import org.elasticsearch.action.admin.indices.settings.put.UpdateSettingsResponse;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesRequestBuilder;
import org.elasticsearch.action.admin.indices.template.get.GetIndexTemplatesResponse;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.indices.IndexMissingException;

/**
 * Created by tianming.mao on 15/2/28.
 */
public class ESWriterPeriphery implements IWriterPeriphery {
    private final static Logger LOG = Logger.getLogger(ESWriterPeriphery.class);
    @Override
    public void rollback(IParam param) {

    }

    @Override
    public void prepare(IParam param, ISourceCounter counter) {
        LOG.info("%% prepare %%");

        String clusterName = param.getValue(ParamKey.clusterName);
        String transportAddress = param.getValue(ParamKey.transportAddress);

        String topicName = param.getValue(ParamKey.topicName);
        String topicType = param.getValue(ParamKey.topicType);

        // get client
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        TransportClient client = new org.elasticsearch.client.transport.TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(transportAddress, 9300));

        // assert that index template exists
        String indexTemplateName = topicName + "_template";
        GetIndexTemplatesRequestBuilder getIndexTemplatesRequest =
                client.admin().indices().prepareGetTemplates(indexTemplateName);
        GetIndexTemplatesResponse getIndexTemplatesResponse = getIndexTemplatesRequest.execute().actionGet();
        LOG.info("getIndexTemplatesResponse.getIndexTemplates().size() = " +
                getIndexTemplatesResponse.getIndexTemplates().size());
        if (getIndexTemplatesResponse.getIndexTemplates().size() != 1)
            throw new AssertionError(indexTemplateName + " does not exist");
        LOG.info(indexTemplateName + " exists");

        if (topicType.equalsIgnoreCase("append")) {
            String date = param.getValue(ParamKey.date);
            if (date == null) {
                throw new AssertionError("parameter 'date' is required when topicType is append");
            }

            String index = topicName + "." + date;

            try {
                LOG.info("delete index " + index + " if exists");
                DeleteIndexRequestBuilder deleteIndexRequest = client.admin().indices().prepareDelete(index);
                DeleteIndexResponse deleteIndexResponse = deleteIndexRequest.execute().actionGet();
            } catch (IndexMissingException e) {
                LOG.info(e.getMessage());
            }
        } else if (topicType.equalsIgnoreCase("full")) {
            // nop
        } else {
            throw new AssertionError("topicType should either be 'append' or 'full'");
        }

        // close client
        client.close();
    }

    @Override
    public void doPost(IParam param, ITargetCounter counter, int faildSize) {
        LOG.info("%% doPost %%");

        String clusterName = param.getValue(ParamKey.clusterName);
        String transportAddress = param.getValue(ParamKey.transportAddress);

        String topicName = param.getValue(ParamKey.topicName);
        String topicType = param.getValue(ParamKey.topicType);

        String index = null;

        if (topicType.equalsIgnoreCase("append")) {
            String date = param.getValue(ParamKey.date);
            if (date == null) {
                throw new AssertionError("parameter 'date' is required when topicType is append");
            }
            index = topicName + "." + date;
        } else if (topicType.equalsIgnoreCase("full")) {
            index = topicName;
        } else {
            throw new AssertionError("topicType should either be 'append' or 'full'");
        }

        Settings settings = ImmutableSettings.settingsBuilder()
                .put("cluster.name", clusterName).build();
        TransportClient client = new org.elasticsearch.client.transport.TransportClient(settings)
                .addTransportAddress(new InetSocketTransportAddress(transportAddress, 9300));

        // optimize index
        OptimizeRequestBuilder optimizeRequest =
                client.admin().indices().prepareOptimize(index)
                        .setMaxNumSegments(1)
                        .setWaitForMerge(true)
                        .setFlush(true)
                        .setForce(true);

        LOG.info("start optimizing " + index);
        long timeStartOptimizing = System.currentTimeMillis();
        OptimizeResponse optimizeResponse = optimizeRequest.execute().actionGet();
        LOG.info("optimized " + index + ". milli-seconds spent: " + (System.currentTimeMillis() - timeStartOptimizing));

        // get another replica shard
        Settings IndexSettings = ImmutableSettings.settingsBuilder()
                .put("number_of_replicas", 1).build();
        UpdateSettingsRequestBuilder updateSettingsRequest =
                client.admin().indices().prepareUpdateSettings(index).setSettings(IndexSettings);
        UpdateSettingsResponse updateSettingsResponse = updateSettingsRequest.execute().actionGet();
        LOG.info("set number_of_replicas to 1");

        client.close();
    }
}