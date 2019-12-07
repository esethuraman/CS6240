package cf.hbase;


import cf.config.HbaseConstants;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;

public class HbaseDao {

    Configuration conf;
    Connection connection;
    Admin admin;

    static final String INFO = HbaseConstants.INFO;
    static final String TABLE_NAME = HbaseConstants.TABLE_NAME;
    static final String COUNTRY = HbaseConstants.COUNTRY;
    static final String WEIGHT = HbaseConstants.WEIGHT;
    static final String FLOW = HbaseConstants.FLOW;

    public HbaseDao() throws IOException {

        conf = HBaseConfiguration.create();
        conf.set(HbaseConstants.QUORUM, HbaseConstants.DNS_NAME);
        conf.set(HbaseConstants.ZK_PORT, HbaseConstants.ZK_PORT_NUMBER);
        conf.set(HbaseConstants.HB_MASTER, HbaseConstants.DNS_NAME);
        conf.set(HbaseConstants.HB_PORT, HbaseConstants.HB_PORT_NUMBER);
        conf.set(HbaseConstants.HB_DIR, HbaseConstants.HB_ROOT_DIR);
        connection = ConnectionFactory.createConnection(conf);
        admin = connection.getAdmin();

//        createTable();

    }

    /**
     * Creates a table in Hbase with relevant column families and qualifiers.
     * @throws IOException
     */
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

    /**
     * For the given input key, append a positive integer starting from 0 and search in Hbase.
     * This is continued until a value is found in Hbase. Each such result is accumulated and
     * returned as a collection of results.
     *
     * Example:
     *      input key : 1234-2019
     *      Hbase search would be like 1234-2019-0, 1234-2019-1, etc
     * @param key a combination of commodityCode and year
     * @return all the export data from Hbase whose rows are prefixed with the input key
     * @throws IOException
     */
    public List<String> readAllForKey(String key) throws IOException {
        List<String> results = new ArrayList<>();

        int i = 0;
        while (true) {
            String thisKey = key + "-" + i;
            String result = readData(thisKey);
            if (result != null) {
                results.add(result);
            } else {
                break;
            }
            i++;
        }

        return results;
    }

    /**
     *
     * @param rowId for which a lookup has to be made in Hbase.
     * @return record from Hbase.
     * @throws IOException
     */
    public String readData(String rowId) throws IOException {
        String rowInfo = null;

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Get g = new Get(rowId.getBytes());
            Result result = table.get(g);

            byte[] flow = result.getValue(INFO.getBytes(), FLOW.getBytes());
            byte[] country = result.getValue(INFO.getBytes(), COUNTRY.getBytes());
            byte[] weight = result.getValue(INFO.getBytes(), WEIGHT.getBytes());

            if ((country != null) && (flow != null) && (weight != null)) {
                rowInfo = new String(flow) + "-" + new String(country) + "-" + bytesToDouble(weight);
            }
        }
        return rowInfo;
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

    /**
     * Read all records from Hbase that is prefixed with the input value.
     *
     * @param rowIdPrefix for which Hbase table is lookup into.
     * @return collection of records with the input prefix from Hbase.
     * @throws IOException
     */
    public List<String> readDataByPrefix(String rowIdPrefix) throws IOException {
        List<String> allResults = new LinkedList<>();

        try (Table table = connection.getTable(TableName.valueOf(TABLE_NAME))) {
            Filter filter = new PrefixFilter(Bytes.toBytes(rowIdPrefix));
            Scan scan = new Scan();
            scan.setFilter(filter);
            System.out.println("Printing results based on prefix.....");
            ResultScanner resultScanner = table.getScanner(scan);

            for (Result result : resultScanner) {
                StringBuilder thisResult = new StringBuilder();

                byte[] f = result.getValue(Bytes.toBytes(INFO), Bytes.toBytes(FLOW));
                byte[] c = result.getValue(Bytes.toBytes(INFO), Bytes.toBytes(COUNTRY));
                byte[] w = result.getValue(Bytes.toBytes(INFO), Bytes.toBytes(WEIGHT));

//                System.out.println("---------------------------------------------------");

                if (new String(f).contains("xport")) {
                    thisResult
                            .append(new String(f)).append("-")
                            .append(new String(c)).append("-")
                            .append(bytesToDouble(w));
                }
                allResults.add(String.valueOf(thisResult));

//                System.out.println("---------------------------------------------------");
//                netResult.append(" RESULT FOR PREFIX : ").append(result);
            }
            resultScanner.close();
            System.out.println("Finished Printing results based on prefix.....");
        }
//        return String.valueOf(netResult);
        return allResults;
    }
}
