package cf;

import cf.mrrep.HbaseCommoditiesMapper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.filecache.DistributedCache;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.*;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.log4j.LogManager;
import org.apache.log4j.Logger;

public class CommodityFlowFinder extends Configured implements Tool {
	private static final Logger logger = LogManager.getLogger(CommodityFlowFinder.class);

	@Override
	public int run(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Replicated Join");
		job.setJarByClass(CommodityFlowFinder.class);

		job.setMapperClass(HbaseCommoditiesMapper.class);
//		job.setReducerClass(CommodityReducer.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

//		job.setGroupingComparatorClass(GroupingComparator.class);
//		job.setSortComparatorClass(KeyComparator.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		// Configure the DistributedCache
		DistributedCache.addCacheFile(new Path(args[0]).toUri(),
				job.getConfiguration());
		DistributedCache.setLocalFiles(job.getConfiguration(), args[0]);
		boolean done  = job.waitForCompletion(true);
		return done ? 0 : 1;
	}

	public static void main(final String[] args) {
		if (args.length != 2) {
			throw new Error("Two arguments required:\n<input-dir> <output-dir>");
		}

		try {
			ToolRunner.run(new CommodityFlowFinder(), args);
		} catch (final Exception e) {
			logger.error("", e);
		}
	}

}
