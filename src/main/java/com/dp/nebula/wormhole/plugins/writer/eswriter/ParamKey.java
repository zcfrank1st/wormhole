package com.dp.nebula.wormhole.plugins.writer.eswriter;

/**
 * Created by tianming.mao on 15/2/28.
 */
public final class ParamKey {
    /*
	 * @name: clusterName
	 * @description: es cluster name
	 * @range:
	 * @mandatory: true
	 * @default: mercury
	 */
    public final static String clusterName = "clusterName";

    /*
	 * @name: transportAddress
	 * @description: es transport address
	 * @range:
	 * @mandatory: true
	 * @default: 10.1.15.60
	 */
    public final static String transportAddress = "transportAddress";

    /*
	 * @name: indexPrefix
	 * @description: es index prefix
	 * @range:
	 * @mandatory: true
	 * @default: myindex
	 */
    public final static String indexPrefix = "indexPrefix";

    /*
	 * @name: indexDate
	 * @description: es index date
	 * @range:
	 * @mandatory: true
	 * @default: 2015-03-09
	 */
    public final static String indexDate = "indexDate";

    /*
	 * @name: indexHour
	 * @description: es index hour
	 * @range:
	 * @mandatory: false
	 * @default: 2015-03-09
	 */
    public final static String indexHour = "indexHour";

    /*
	 * @name: type
	 * @description: es type name
	 * @range:
	 * @mandatory: true
	 * @default: 00
	 */
    public final static String type = "type";

    /*
	 * @name: fields
	 * @description: fields to es
	 * @range:
	 * @mandatory: true
	 * @default: aaa,bbb
	 */
    public final static String fields = "fields";

    /*
    * @name: bulkSize
    * @description: es bulk size
    * @range:
    * @mandatory: true
    * @default: 10000
    */
    public final static String bulkSize = "bulkSize";
    /*
          * @name: concurrency
          * @description: concurrency of the job
          * @range:1-10
          * @mandatory: false
          * @default:1
          */
    public final static String concurrency = "concurrency";
}