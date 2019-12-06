package cf.config;

import org.apache.hadoop.hbase.HBaseConfiguration;

public class HbaseConstants {
    public static final String INFO = "info";
    public static final String TABLE_NAME = "trade_commodities_FULL";
    public static final String COUNTRY = "country";
    public static final String WEIGHT = "weight";
    public static final String FLOW = "flow";

    public static final String QUORUM = "hbase.zookeeper.quorum";
    public static final String ZK_PORT = "hbase.zookeeper.property.clientPort";
    public static final String HB_MASTER = "hbase.master";
    public static final String HB_PORT = "hbase.master.port";
    public static final String HB_DIR = "hbase.rootdir";

    public static final String DNS_NAME = "ec2-54-205-173-124.compute-1.amazonaws.com";
    public static final String ZK_PORT_NUMBER = "2181";
    public static final String HB_PORT_NUMBER = "60000";
    public static final String HB_ROOT_DIR = "s3://cs6240-hbase/expanded";
}
