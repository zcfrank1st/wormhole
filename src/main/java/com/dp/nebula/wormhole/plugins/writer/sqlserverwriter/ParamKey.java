package com.dp.nebula.wormhole.plugins.writer.sqlserverwriter;

/**
 * Created by zcfrank1st on 7/5/15.
 */
public class ParamKey {
    /*
		 * @name: connectProps
		 * @description: id of SqlServer database's connect string properties
		 * @range: if name is testProp, then you can set testProp.ip, testProp.port and so on in the WORMHOLE_CONNECT_FILE
		 * @mandatory: false
		 * @default:
		 */
    public final static String connectProps = "connectProps";
    /*
      * @name: ip
      * @description: SqlServer database's ip address
      * @range:
      * @mandatory: false
      * @default:
      */
    public final static String ip = "ip";
    /*
       * @name: port
       * @description: SqlServer database's port
       * @range:
       * @mandatory: false
       * @default:1433
       */
    public final static String port = "port";
    /*
       * @name: dbname
       * @description: SqlServer database's name
       * @range:
       * @mandatory: false
       * @default:
       */
    public final static String dbname = "dbname";
    /*
       * @name: username
       * @description: SqlServer database's login name
       * @range:
       * @mandatory: false
       * @default:
       */
    public final static String username = "username";
    /*
       * @name: password
       * @description: SqlServer database's login password
       * @range:
       * @mandatory: false
       * @default:
       */
    public final static String password = "password";

    public final static String url = "url";

    /*
       * @name: encoding
       * @description: SqlServer database's encode
       * @range: UTF-8|GBK|GB2312
       * @mandatory: false
       * @default: UTF-8
       */
    public final static String encoding = "encoding";

    /*
      * @name: concurrency
      * @description: concurrency of the job
      * @range: 1-10
      * @mandatory: false
      * @default: 1
      */
    public final static String concurrency = "concurrency";

		/*
	       * @name: tableName
	       * @description: table to export data
	       * @range:
	       * @mandatory: false
	       * @default:
	       */

    public final static String tableName = "tableName";

    /*
       * @name: columns
       * @description: columns to be selected, default is *
       * @range:
       * @mandatory: false
       * @default: *
       */
    public final static String columns = "columns";

    public final static String sqlserverParams = "sqlserverParams";

    // 前置操作 danger
    public final static String pre = "pre";
}
