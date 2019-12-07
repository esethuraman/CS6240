package cf.mrrep;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommodityReducer extends Reducer<Text, Text, Text, Text> {

    /**
     * A naive reducer which is solely used for the purpose of sorting.
     */
    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        for (Text value : values){
            context.write(key, value);
        }
    }
}
