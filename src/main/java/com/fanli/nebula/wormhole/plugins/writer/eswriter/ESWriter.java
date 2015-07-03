//package com.dp.nebula.wormhole.plugins.writer.eswriter;
//
//import com.dp.nebula.wormhole.common.AbstractPlugin;
//import com.dp.nebula.wormhole.common.interfaces.ILine;
//import com.dp.nebula.wormhole.common.interfaces.ILineReceiver;
//import com.dp.nebula.wormhole.common.interfaces.IWriter;
//import org.apache.log4j.Logger;
//import org.elasticsearch.action.bulk.BulkRequestBuilder;
//import org.elasticsearch.action.bulk.BulkResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.common.settings.ImmutableSettings;
//import org.elasticsearch.common.settings.Settings;
//import org.elasticsearch.common.transport.InetSocketTransportAddress;
//import org.elasticsearch.common.xcontent.XContentBuilder;
//import org.elasticsearch.node.Node;
//
//import java.io.IOException;
//import java.util.HashSet;
//import java.util.Set;
//
//import static org.elasticsearch.common.xcontent.XContentFactory.jsonBuilder;
//
///**
// * Created by tianming.mao on 15/2/28.
// */
//public class ESWriter extends AbstractPlugin implements IWriter {
//
//    private final static Logger LOG = Logger.getLogger(ESWriter.class);
//
//    private Node node = null;
//    private Client client = null;
//    private String index = null;
//
//    private String clusterName = null;
//    private String transportAddress = null;
//    private String topicName = null;
//    private String topicType = null;
//    private String esType = null;
//    private int bulkSize = -1;
//    private String idField = null;
//    private int idFieldPos = -1;
//    private String[] fieldList = null;
//    private Set<String> arrayFieldsSet = new HashSet<String>();
//
//    @Override
//    public void init(){
//        LOG.info("%% init %%");
//
//        clusterName = getParam().getValue(ParamKey.clusterName);
//        transportAddress = getParam().getValue(ParamKey.transportAddress);
//        topicName = getParam().getValue(ParamKey.topicName);
//        topicType = getParam().getValue(ParamKey.topicType);
//        esType = getParam().getValue(ParamKey.esType);
//        bulkSize = getParam().getIntValue(ParamKey.bulkSize);
//
//        if (topicType.equalsIgnoreCase("append")) {
//            String date = getParam().getValue(ParamKey.date);
//            if (date == null) {
//                throw new AssertionError("parameter 'date' is required when topicType is append");
//            }
//            index = topicName + "." + date;
//            String hour = getParam().getValue(ParamKey.hour, "");
//            if (!hour.isEmpty()) {
//                index = index + "." + hour;
//            }
//        } else if (topicType.equalsIgnoreCase("full")) {
//            index = topicName;
//        } else {
//            throw new AssertionError("topicType should either be 'append' or 'full'");
//        }
//
//        idField = getParam().getValue(ParamKey.idField, "");
//        LOG.info("idField: " + idField);
//
//        String fields = getParam().getValue(ParamKey.fields);
//        if (fields == null) throw new AssertionError();
//        LOG.info("fields: " + fields);
//
//        fieldList = fields.split(",");
//        StringBuilder sb = new StringBuilder().append("field list: \n");
//        for (int i = 0; i < fieldList.length; i++) {
//            fieldList[i] = fieldList[i].trim();
//            sb.append("%%").append(fieldList[i]).append("%%\n");
//        }
//        LOG.info(sb.toString());
//
//        if (!idField.isEmpty()) {
//            for (int i = 0; i < fieldList.length; i++) {
//                if (fieldList[i].equals(idField)) {
//                    idFieldPos = i;
//                    break;
//                }
//            }
//            if (idFieldPos < 0) {
//                throw new AssertionError("idField '" + idField + "' doesn't exist in field list");
//            }
//        }
//
//        String arrayFieldsValue = getParam().getValue(ParamKey.arrayFields, "");
//        String[] arrayFieldsArray = arrayFieldsValue.split(",");
//        StringBuilder sb2 = new StringBuilder().append("array fields list: \n");
//        for (int i = 0; i < arrayFieldsArray.length; i++) {
//            String trimmedField = arrayFieldsArray[i].trim();
//            if (!trimmedField.isEmpty()) {
//                arrayFieldsSet.add(trimmedField);
//                sb2.append("%%").append(trimmedField).append("%%\n");
//            }
//        }
//        LOG.info(sb2.toString());
//
//    }
//
//    @Override
//    public void connection() {
//        Settings settings = ImmutableSettings.settingsBuilder()
//                .put("cluster.name", clusterName).build();
//        client = new org.elasticsearch.client.transport.TransportClient(settings)
//                .addTransportAddress(new InetSocketTransportAddress(transportAddress, 9300));
//    }
//
//    @Override
//    public void write(ILineReceiver lineReceiver) {
//        LOG.info("%% write %%");
//        ILine line = null;
//        BulkRequestBuilder bulkRequest = client.prepareBulk();
//        int linesInBulkRequest = 0;
//
//        while((line = lineReceiver.receive()) != null) {
//            if (line.getFieldNum() != fieldList.length) throw new AssertionError();
//
//            try {
//                XContentBuilder jb = jsonBuilder();
//                jb.startObject();
//                for (int i = 0; i < fieldList.length; i++) {
//                    if (arrayFieldsSet.contains(fieldList[i])) { // array field
//                        // split by '\u0002'
//                        String val = line.getField(i);
//                        if (val == null) {
//                            jb.field(fieldList[i], val);
//                        } else {
//                            String[] elements = val.split("\u0002");
//                            jb.field(fieldList[i], elements);
//                        }
//                    } else { // normal field
//                        jb.field(fieldList[i], line.getField(i));
//                    }
//                }
//                jb.endObject();
//                String esDocId = null;
//                if (!idField.isEmpty()) {
//                    esDocId = line.getField(idFieldPos);
//                }
//
//                bulkRequest.add(client.prepareIndex(index, esType, esDocId).setSource(jb));
//                linesInBulkRequest++;
//
//                if (linesInBulkRequest == bulkSize) {
//                    triggerBulkRequest(bulkRequest);
//                    linesInBulkRequest = 0;
//                    bulkRequest = client.prepareBulk();
//                }
//            } catch (IOException e) {
//                e.printStackTrace();
//            }
//        }
//
//        if (linesInBulkRequest != 0) {
//            triggerBulkRequest(bulkRequest);
//        }
//    }
//
//    private void triggerBulkRequest(BulkRequestBuilder bulkRequest) {
//        BulkResponse bulkResponse = bulkRequest.execute().actionGet();
//
//        if (bulkResponse.hasFailures()) {
//            LOG.warn("bulk request has errors" + bulkResponse.buildFailureMessage());
//        }
//
//        getMonitor().increaseSuccessLine(bulkRequest.numberOfActions());
//        LOG.info("docs imported: " + bulkRequest.numberOfActions());
//    }
//
//    @Override
//    public void commit() {
//        LOG.info("%% commit %%");
//    }
//
//    @Override
//    public void finish(){
//        LOG.info("%% finish %%");
//        client.close();
//    }
//}