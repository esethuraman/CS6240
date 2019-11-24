import java.io.IOException;
import java.util.*;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CommoditiesFlowFinder extends Configured implements Tool {
    private static final Logger logger = LogManager.getLogger(CommoditiesFlowFinder.class);


    public static class TokenizerMapper
            extends Mapper<Object, Text, Text, Text>  {

        /*
        Each input value fed in to mapper is of the form (followerId,userId). Note that followerId
        is in turn an userId.
         */
        @Override
        public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			/*Tokenizing the input value based on comma. For this dataset, there will always be two
			tokens. Those tokens are followerId and userId.*/

//			id,country_or_area,year,comm_code,commodity,flow,trade_usd,weight_kg,quantity_name,quantity,category
            String[] contents = value
                    .toString()
                    .replace(", ", " ")
                    .split(",");

            try {
                CommodityInfo commodityInfo = getCommodityInfo(contents);
                if (commodityInfo != null) {
                    Text mapKey = getMapperEmitKey(commodityInfo);
                    Text mapValue = getMapperEmitValue(commodityInfo);
                    context.write(mapKey, mapValue);

                }
            } catch (Exception e) {
//                No quantity type commodities are silently ignored using exception
            }


        }

        private Text getMapperEmitKey(CommodityInfo commodityInfo) {
            return new Text(commodityInfo.getCode() + "-" + commodityInfo.getYear());
        }

        private Text getMapperEmitValue(CommodityInfo commodityInfo) {
            return new Text(commodityInfo.getFlow() + "-" +commodityInfo.getCountry());
        }

        private CommodityInfo getCommodityInfo(String[] contents) {
            CommodityInfo info = null;
            if((contents.length > 0) && (contents[0].length() > 0)) {
                info = new CommodityInfo();
                info.setCountry(contents[1]);
                info.setYear(Integer.parseInt(contents[2]));
                info.setCode(contents[3]);
//                info.setCommodity(contents[4]);
                info.setFlow(contents[4]);
                info.setAmoundInUSD(Long.parseLong(contents[5]));
                info.setWeight(Double.parseDouble(contents[6]));
                info.setQuantityName(contents[7]);
                info.setQuantity(Double.parseDouble(contents[8]));
                info.setCategory(contents[9]);
                info.setWeight(contents.length);
            }
            return info;
        }

    }

    public static class TriadReducer extends Reducer<Text, Text, Text, Text> {

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

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Triad");
        job.setJarByClass(CommoditiesFlowFinder.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

        job.setMapperClass(TokenizerMapper.class);
        job.setReducerClass(TriadReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
//        job.setNumReduceTasks(0);
        FileInputFormat.addInputPath(job, new Path("/home/elavazhagan/Documents/GraduateCourse/Fall19/MR/project/CS6240/MR_RSJoin/sample_input"));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public static void main(final String[] args) {
        if (args.length != 3) {
            throw new Error("Three arguments required:" +
                    "\n<input-dir> <intermediate-output-dir> <final-output-dir>");
        }

        try {
//			This run contains a job that finds Path2
            ToolRunner.run(new CommoditiesFlowFinder(), args);
//			This run contains a job that finds the x->y->z->x connection
//            ToolRunner.run(new TriadClosure(), args);
        } catch (final Exception e) {
            logger.error("", e);
        }
    }

}
