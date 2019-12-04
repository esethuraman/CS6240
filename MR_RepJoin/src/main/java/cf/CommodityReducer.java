package cf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import java.io.IOException;

public class CommodityReducer extends Reducer<Text, Text, Text, Text> {

    @Override
    public void reduce(final Text key, final Iterable<Text> values, final Context context) throws IOException, InterruptedException {
        for (Text value : values){
            context.write(key, value);
        }
    }
}
