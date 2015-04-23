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
     * @name: topicName
     * @description: 代表一类数据主题，比如流量、用户画像等
     * @range:
     * @mandatory: true
     * @default: mytopic
     */
    public final static String topicName = "topicName";

    /*
     * @name: topicType
     * @description: 代表数据主题的类型；
     *               chronic表示随时间产生的数据，比如流量，每天产生并堆积，历史数据不会修改；
     *               full表示全量数据，比如用户画像，全量数据很大，但每天会修改小部分数据
     *               对es来说，chronic类的topic表现为每天一个index
     *                        full类的top表现为仅有一个index，每天apply增量
     * @range:
     * @mandatory: true
     * @default: chronic
     */
    public final static String topicType = "topicType";

    /*
	 * @name: date
	 * @description: es index date; required when topicType is chronic
	 * @range:
	 * @mandatory: false
	 * @default: 2015-03-09
	 */
    public final static String date = "date";

    /*
	 * @name: estype
	 * @description: es type name
	 * @range:
	 * @mandatory: true
	 * @default: docs
	 */
    public final static String estype = "estype";

    /*
	 * @name: isFirstFieldUsedAsID
	 * @description: is first field used as es doc id
	 * @range:
	 * @mandatory: false
	 * @default: false
	 */
    public final static String isFirstFieldUsedAsID = "isFirstFieldUsedAsID";

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
    * @range: 1-10
    * @mandatory: false
    * @default: 1
    */
    public final static String concurrency = "concurrency";
}