package hbase;

import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.PrefixFilter;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;

public class HBaseReader {

    String readData() throws IOException {
        return new HbaseDao().readData("010591-2016-1926850.0");
    }

    String readDataByPrefix() throws IOException {
        return new HbaseDao().readDataByPrefix();
    }

    List<String> readAllForKey() throws IOException {
        return new HbaseDao().readAllForKey("010511-2016");
    }
}
