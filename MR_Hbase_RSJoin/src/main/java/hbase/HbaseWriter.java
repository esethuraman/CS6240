package hbase;

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

import javax.swing.plaf.synth.SynthOptionPaneUI;
import java.io.IOException;

public class HbaseWriter extends Configured implements Tool {

    private static final Logger logger = LogManager.getLogger(HbaseWriter.class);
    HbaseWriter() throws IOException {

    }

    public static void main(String[] args) throws Exception {
//        System.out.println("MAIN INVOKED ");
//        ToolRunner.run(new HbaseWriter(), args);

        System.out.println("WRITE DONE.. NOW GONNA READ....");
        String res = new HBaseReader().readData();
        System.out.println("READ RESULT  " + res);
        logger.info("READ RESULT  " + res);
        System.out.println("donee,,....");
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
