import org.apache.hadoop.conf.Configuration;

import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HBaseAdmin;

import java.io.IOException;

public class HBaseHandson
{
    public static void main(String[] args) throws Exception
    {
        Configuration conf = HBaseConfiguration.create();
////        conf.set("hbase.zookeeper.quorum", "");
//        conf.set("hbase.zookeeper.property.clientPort", "2181");
//        conf.set("hbase.master", "");
//        conf.set("hbase.master.port", "60000");
//        conf.set("hbase.rootdir", "s3://ahbase-sample");
        Connection connection = ConnectionFactory.createConnection(conf);
        Admin admin = connection.getAdmin();

        // Instantiating table descriptor class
        HTableDescriptor tableDescriptor = new
                HTableDescriptor(TableName.valueOf("emp"));

        // Adding column families to table descriptor
        tableDescriptor.addFamily(new HColumnDescriptor("personal"));
        tableDescriptor.addFamily(new HColumnDescriptor("professional"));

        // Execute the table through admin
        admin.createTable(tableDescriptor);
        System.out.println(" Table created ");
        for (TableName name : admin.listTableNames()) {
            System.out.println("Table name" + name.getNameAsString());
        }

//        HBaseConfiguration hconfig = new HBaseConfiguration(new Configuration());
//        HTableDescriptor htable = new HTableDescriptor("User");
//        htable.addFamily( new HColumnDescriptor("Id"));
//        htable.addFamily( new HColumnDescriptor("Name"));
//        System.out.println( "Connecting..." );
//        HBaseAdmin hbase_admin = new HBaseAdmin( hconfig );
//        System.out.println( "Creating Table..." );
//        hbase_admin.createTable( htable );
//        System.out.println("Done!");
    }
}