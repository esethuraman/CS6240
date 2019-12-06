package cf;

import cf.hbase.HbaseDao;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.*;
import java.util.HashMap;
import java.util.Map;
import java.util.StringTokenizer;

public class HbaseCommoditiesMapper extends Mapper<Object, Text, Text, Text> {
    //		private HashSet<String> exportRecords = new HashSet<String>();

    static HbaseDao dao;

    static {
        try {
            dao = new HbaseDao();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private String recordStr;
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
        final StringTokenizer itr = new StringTokenizer(value.toString());
        while (itr.hasMoreTokens()) {
            try {
                recordStr = itr.nextToken("\n");
                recordStr = recordStr.replace(", ", " ");
                String [] recordStrList = recordStr.split(",");

                String importCountry = recordStrList[0];
                String year = recordStrList[1];
                String commCode = recordStrList[2];
                String flow = recordStrList[4];
                String importWeight = recordStrList[6];

                String hbaseKeyPrefix = String.join("-", commCode, year);
                System.out.println("=====> THIS ONE " +  hbaseKeyPrefix + " -- " + flow);
                // FILTER_IMPORT
                if (flow.contains("mport")) {
//                System.out.println(" IMPORT DATA .... " + hbaseKeyPrefix);
                    for (String exportData: dao.readAllForKey(hbaseKeyPrefix))   {
//                    System.out.println(" EXPORT DATA .... ");
                        try {
                            String[] values = exportData.split("-");
                            String exportCountry = values[1];
                            String exportWeight = values[2];

                            String k = commCode+"-"+year+"-"+ importWeight;
                            String v = String.join("-", exportCountry, exportWeight,
                                    importCountry, importWeight);
                            context.write(new Text(k), new Text(v));

                            System.out.println(k + " ---- " + v);
                        } catch (ArrayIndexOutOfBoundsException e) {
//                        context.write(new Text("EXCEPTION"), new Text(hbaseKeyPrefix + " Export data-- " + exportData + " -- Current record " + recordStr));

                        }

                    }
                }
            } catch (Exception e) {
                System.out.println(" error.....");
            }
        }
    }
}

