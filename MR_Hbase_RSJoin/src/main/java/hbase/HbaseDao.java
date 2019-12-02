package hbase;

import models.CommodityInfo;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;

public class HbaseDao {

    Configuration conf;
    Connection connection;
    Admin admin;

    static final String INFO = "info";
    static final String TABLE_NAME = "commodities_expanded";
    static final String COUNTRY = "country";
    static final String WEIGHT = "weight";
    static final String FLOW = "flow";

    HbaseDao() throws IOException {

        String dnsName = "ec2-3-81-9-217.compute-1.amazonaws.com";
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", dnsName);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", dnsName);
        conf.set("hbase.master.port", "60000");
        conf.set("hbase.rootdir", "s3://cs6240-hbase/expanded");
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();

//        createTable();

    }

    private void createTable() throws IOException {
        /*
        https://stackoverflow.com/questions/35661843/htabledescriptortable-in-hbase-is-deprecated-and-alternative-for-that
         */
        TableName table = TableName.valueOf(TABLE_NAME);

        TableDescriptorBuilder tableDescriptorBuilder = TableDescriptorBuilder.newBuilder(table);
        ColumnFamilyDescriptorBuilder familyDescriptorBuilder = ColumnFamilyDescriptorBuilder
                .newBuilder(Bytes.toBytes(INFO));
        tableDescriptorBuilder.setColumnFamily(familyDescriptorBuilder.build());


//        HTableDescriptor tableDescriptor = new HTableDescriptor(table);
//        tableDescriptor.addFamily(new HColumnDescriptor(INFO));

        admin.createTable(tableDescriptorBuilder.build());
//        admin.createTable(tableDescriptor);
    }

    public void writeData(String rowId, CommodityInfo commodityInfo) throws IOException {

        System.out.println("call to write started...");
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {

            Put p = new Put(rowId.getBytes());

            p.addColumn(INFO.getBytes(), FLOW.getBytes(), commodityInfo.getFlow().getBytes());
            p.addColumn(INFO.getBytes(), COUNTRY.getBytes(), commodityInfo.getCountry().getBytes());
            p.addColumn(INFO.getBytes(), WEIGHT.getBytes(), doubleToBytes(commodityInfo.getWeight()));

            table.put(p);
            System.out.println("Data inserted with key : " + rowId);
        }

        System.out.println("call to write finished...");

//        System.out.println("Data successsfully inserted.....");
//        System.out.println("******************************************");
    }


    public String readData(String rowId) throws IOException {
        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get g = new Get(rowId.getBytes());
            Result result = table.get(g);
            System.out.println("----------------------------------------------------");

            System.out.println( " DATA FOR ROW : " + new String(rowId));

            byte[] flow = result.getValue(INFO.getBytes(), FLOW.getBytes());
            byte[] country = result.getValue(INFO.getBytes(), COUNTRY.getBytes());
            byte[] weight = result.getValue(INFO.getBytes(), WEIGHT.getBytes());

            return String.format(" COUNTRY: %s%nFLOW: %s%nWEIGHT: %s",
                    new String(country),
                    new String(flow),
//                    new String(weight));
                    bytesToDouble(weight));

        }
    }

    private double bytesToDouble(byte[] weight) {
        return ByteBuffer.wrap(weight).getDouble();
    }

    /*
    https://stackoverflow.com/questions/2905556/how-can-i-convert-a-byte-array-into-a-double-and-back
     */
    private byte[] doubleToBytes(double weight) {
        byte[] bytes = new byte[8];
        ByteBuffer.wrap(bytes).putDouble(weight);
        return bytes;
    }

    public String readDataByPrefix() throws IOException {
        StringBuilder netResult = new StringBuilder();

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Filter filter = new PrefixFilter(Bytes.toBytes("010410-2016"));
            Scan scan = new Scan();
            scan.setFilter(filter);
            System.out.println("Printing results based on prefix.....");
            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {

                byte[] f = result.getValue(Bytes.toBytes(INFO), Bytes.toBytes(FLOW));
                byte[] c = result.getValue(Bytes.toBytes(INFO), Bytes.toBytes(COUNTRY));
                byte[] w = result.getValue(Bytes.toBytes(INFO), Bytes.toBytes(WEIGHT));

//                System.out.println("---------------------------------------------------");
                netResult
                        .append("\n")
                        .append(new String(f)).append("  <> ")
                        .append(new String(c)).append(" <> ")
                        .append(bytesToDouble(w));
//                System.out.println("---------------------------------------------------");
//                netResult.append(" RESULT FOR PREFIX : ").append(result);
            }
            System.out.println("Finished Printing results based on prefix.....");
        }
        return String.valueOf(netResult);
    }
}
