import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseHandson
{

    static byte[] FAMILY_PERSONAL = "personal".getBytes();
    static byte[] FAMILY_PROFESSIONAL = "professional".getBytes();
    static byte[] QUALIFIER_NAME = "name".getBytes();
    static byte[] QUALIFIER_CITY = "city".getBytes();
    static byte[] row1 = Bytes.toBytes("ela");
    static byte[] row2 = Bytes.toBytes("hari");

    public static void main(String[] args) throws Exception
    {
        String dnsName = "ec2-3-81-9-217.compute-1.amazonaws.com";
        Configuration conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", dnsName);
        conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("hbase.master", dnsName);
        conf.set("hbase.master.port", "60000");
        conf.set("hbase.rootdir", "s3://hbase-sample/first");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        createTable(admin);
    
        createData(connection, admin);

        readData(connection);
    }

    private static void readAllRows(Connection connection) throws IOException {
        Scan scan = new Scan();
        scan.setCaching(20);

        try(Table table = connection.getTable(TableName.valueOf("empl1"))){
            // Scanning the required columns
            scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("name"));
            scan.addColumn(Bytes.toBytes("personal"), Bytes.toBytes("city"));

            // Getting the scan result
            ResultScanner scanner = table.getScanner(scan);

            System.out.println(" READING FROM ALL ROWS....");
            // Reading values from scan result
            for (Result result = scanner.next(); result != null; result = scanner.next())

                System.out.println("Found row : " + result);
            System.out.println(" READ ALL ROWS...");
            //closing the scanner
            scanner.close();
        }
//        s
    }
    private static void readData(Connection connection) throws IOException {

        try (Table table = connection.getTable(TableName.valueOf("Names"))) {


            byte[][] rows = new byte[][]{row1, row2};

            for (byte[] row : rows) {
                Get g = new Get(row);
                Result result = table.get(g);
                System.out.println("----------------------------------------------------");

                System.out.println( " DATA FOR ROW : " + new String(row));
                byte[] value = result.getValue(FAMILY_PERSONAL, QUALIFIER_NAME);
                System.out.println("VALUE : ==> " + new String(value));

                value = result.getValue(FAMILY_PERSONAL, QUALIFIER_CITY);
                System.out.println("VALUE : ==> " + new String(value));

                value = result.getValue(FAMILY_PROFESSIONAL, QUALIFIER_NAME);
                System.out.println("VALUE : ==> " + new String(value));

                value = result.getValue(FAMILY_PROFESSIONAL, QUALIFIER_CITY);
                System.out.println("VALUE : ==> " + new String(value));

                System.out.println("----------------------------------------------------");
            }

        }
    }

    private static void listTables(Admin admin) throws IOException {
        System.out.println(" Table created ");
        for (TableName name : admin.listTableNames()) {
            System.out.println("Table name  " + name.getNameAsString());
        }
        System.out.println("******************************************");
    }

    public static void createData(Connection connection, Admin admin) throws IOException {
       try(Table table = connection.getTable(TableName.valueOf("Names"))) {

           Put p = new Put(row1);
           p.addImmutable(FAMILY_PERSONAL, QUALIFIER_NAME, Bytes.toBytes("ELAVAZHAGAN SETHURAMAN"));
           p.addImmutable(FAMILY_PROFESSIONAL, QUALIFIER_NAME, Bytes.toBytes("ELA"));
           p.addImmutable(FAMILY_PERSONAL, QUALIFIER_CITY, Bytes.toBytes("THIRUTHURAIPOONDI"));
           p.addImmutable(FAMILY_PROFESSIONAL, QUALIFIER_CITY, Bytes.toBytes("BOSTON"));
           table.put(p);

           p = new Put(row2);
           p.addImmutable(FAMILY_PERSONAL, QUALIFIER_NAME, Bytes.toBytes("HARIPRASATH SIVANDANAM"));
           p.addImmutable(FAMILY_PROFESSIONAL, QUALIFIER_NAME, Bytes.toBytes("HARI"));
           p.addImmutable(FAMILY_PERSONAL, QUALIFIER_CITY, Bytes.toBytes("BENGALURU"));
           p.addImmutable(FAMILY_PROFESSIONAL, QUALIFIER_CITY, Bytes.toBytes("BOSTON"));
           table.put(p);

           System.out.println("Data successsfully inserted.....");
           System.out.println("******************************************");
       }
    }

    private static void createTable(Admin admin) throws IOException {
        TableName table1 = TableName.valueOf("Names");

        HTableDescriptor tableDescriptor = new HTableDescriptor(table1);
        tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_PERSONAL));
        tableDescriptor.addFamily(new HColumnDescriptor(FAMILY_PROFESSIONAL));
        admin.createTable(tableDescriptor);


    }
}