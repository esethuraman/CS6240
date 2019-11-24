import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

public class CommoditiesReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        List<CommodityInfo> exports = new LinkedList<>();
        List<CommodityInfo> imports = new LinkedList<>();

        for (Text value : values) {
            CommodityInfo info = parseReducerValue(value);
            if (info.getFlow().equals("Export")) {
                exports.add(info);
            } else {
                imports.add(info);
            }
        }

        for (CommodityInfo exportInfo : exports) {
            for (CommodityInfo importInfo : imports){
                Text value = new Text(exportInfo.getCountry() + "-" + importInfo.getCountry());
                context.write(key, value);
            }
        }



    }

    private CommodityInfo parseReducerValue(Text value) {
        CommodityInfo info = new CommodityInfo();
        String[] contents = value.toString().split("-");
        info.setFlow(contents[0]);
        info.setCountry(contents[1]);
        return info;
    }
}