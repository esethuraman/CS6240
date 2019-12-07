package hbase;

import jdk.internal.dynalink.linker.LinkerServices;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.List;

public class HbaseWriter extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(HbaseWriter.class);
    HbaseWriter() throws IOException {

    }

    public static void main(String[] args) throws Exception {
        System.out.println("MAIN INVOKED ");
        new HbaseDao().createTable();

        ToolRunner.run(new HbaseWriter(), args);

        System.out.println("WRITE DONE.. NOW GONNA READ....");
    }

    @Override
    public int run(String[] args) throws Exception {
        final Configuration jconf = getConf();
        final Job job = Job.getInstance(jconf, "Hbase Data Writer");
        job.setJarByClass(HbaseWriter.class);
        final Configuration jobConf = job.getConfiguration();
        jobConf.set("mapreduce.output.textoutputformat.separator", "\t");

        job.setMapperClass(PopulateHBaseMapper.class);
        job.setNumReduceTasks(0);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));

        int res = job.waitForCompletion(true) ? 0 : 1;
        System.out.println("JOB COMPLETED " + res);
        return res;
    }
}
