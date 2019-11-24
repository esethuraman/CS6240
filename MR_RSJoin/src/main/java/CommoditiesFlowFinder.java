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

    @Override
    public int run(final String[] args) throws Exception {
        final Configuration conf = getConf();
        final Job job = Job.getInstance(conf, "Twitter Triad");
        job.setJarByClass(CommoditiesFlowFinder.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

        job.setMapperClass(CommoditiesMapper.class);
        job.setReducerClass(CommoditiesReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);
        job.setGroupingComparatorClass(CommodityIdComparator.class);
        job.setSortComparatorClass(YearComparator.class);
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
