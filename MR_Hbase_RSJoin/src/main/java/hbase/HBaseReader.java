package hbase;

import java.io.IOException;

public class HBaseReader {

    String readData() throws IOException {
        return new HbaseDao().readData("010519-2016-2908.0");
    }
}
