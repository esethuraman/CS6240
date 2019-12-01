import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseHandson
{
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

//        createTable(admin);
    
//        listTables(admin);

//        createData(connection, admin);

        readData(connection);

//        readAllRows(connection);
        

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

        try (Table table = connection.getTable(TableName.valueOf("Table1"))) {
            String family1 = "Family1";
            String qualifier1 = "qualifier1";
            byte[] row1 = Bytes.toBytes("row1");

            Get g = new Get(row1);
            Result result = table.get(g);
            byte[] value = result.getValue(family1.getBytes(), qualifier1.getBytes());

            System.out.println("VALUE : ==> " + new String(value));
//            Get get = new Get(Bytes.toBytes("row1"));
//
//            Result result = table.get(get);
//
//            byte[] name = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("name"));
//
//            byte[] city = result.getValue(Bytes.toBytes("personal"), Bytes.toBytes("city"));
//
//            byte[] lecturer = result.getValue(Bytes.toBytes("professional"), Bytes.toBytes("lecturer"));
//
//            System.out.println("READING DATA ..... ");
//            System.out.println(
//                    "Name " + Bytes.toString(name) + "\n"
//                    + "City " + Bytes.toString(city) + "\n"
//                    + "Lecturer " + Bytes.toString(lecturer)
//            );
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
       try(Table table = connection.getTable(TableName.valueOf("Table1"))) {

           String family1 = "Family1";
           String qualifier1 = "qualifier1";
           byte[] row1 = Bytes.toBytes("row1");
           Put p = new Put(row1);
           p.addImmutable(family1.getBytes(), qualifier1.getBytes(), Bytes.toBytes("cell_data"));
           table.put(p);


           //           Put p = new Put(Bytes.toBytes("row1"));

//           Put put = new Put(Bytes.toBytes("row1"));

//           put.addColumn(Bytes.toBytes("personal"),
//                   Bytes.toBytes("name"),
//                   Bytes.toBytes("Mapreduce"));
//
//           put.addColumn(Bytes.toBytes("personal"),
//                   Bytes.toBytes("city"),
//                   Bytes.toBytes("Boston"));
//
//           put.addColumn(Bytes.toBytes("professional"),
//                   Bytes.toBytes("lecturer"),
//                   Bytes.toBytes("Mirek"));
//
//           put.addColumn(Bytes.toBytes("professional"),
//                   Bytes.toBytes("lecturer"),
//                   Bytes.toBytes("XYZ"));


           System.out.println("Data successsfully inserted.....");
           System.out.println("******************************************");
       }
    }

    private static void createTable(Admin admin) throws IOException {
        TableName table1 = TableName.valueOf("Table1");
        String family1 = "Family1";
        String family2 = "Family2";

        HTableDescriptor tableDescriptor = new HTableDescriptor(table1);
        tableDescriptor.addFamily(new HColumnDescriptor(family1));
        tableDescriptor.addFamily(new HColumnDescriptor(family2));
        admin.createTable(tableDescriptor);

       /* // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf("e1"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));


        // Execute the table through admin
        admin.createTable(tableDescriptor);*/
    }
}