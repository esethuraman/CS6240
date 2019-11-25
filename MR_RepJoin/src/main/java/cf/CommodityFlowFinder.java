package cf;

import java.io.*;
import java.util.*;

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

	// Mapper class performing the map tasks to join on comm_code and year
	public static class CommodityMapper extends Mapper<Object, Text, Text, Text> {
		private HashSet<String> exportRecords = new HashSet<String>();
		private String recordStr;

			@Override
			public void setup(Context context) throws IOException,
					InterruptedException {
				try {
					Path[] files = DistributedCache.getLocalCacheFiles(context
							.getConfiguration());

					if (files == null || files.length == 0) {
						throw new RuntimeException(
								"Edges not set in DistributedCache");
					}

					// Read all files in the DistributedCache
					for (Path p : files) {
						BufferedReader rdr = new BufferedReader(
								new InputStreamReader((new FileInputStream(
												new File(p.getName())))));

						// country_or_area,year,comm_code,commodity,flow,trade_usd,weight_kg,quantity_name,quantity,category
						String line;
						// For each record in the file
						while ((line = rdr.readLine()) != null) {

							line = line.replace(", ", " ");
							logger.info("TESTING: "+line);
							String year = line.split(",")[1];
							String commCode = line.split(",")[2];
							String flow = line.split(",")[4];

							// FILTER_EXPORT
							if (flow.equals("Export")) {
								exportRecords.add(line);
							}
						}
					}

				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			}

		@Override
		public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			final StringTokenizer itr = new StringTokenizer(value.toString());
			while (itr.hasMoreTokens()) {
				recordStr = itr.nextToken("\n");
				recordStr = recordStr.replace(", ", " ");
				String year = recordStr.split(",")[1];
				String commCode = recordStr.split(",")[2];
				String flow = recordStr.split(",")[4];

				// FILTER_IMPORT
				if (flow.equals("Import")) {
					Iterator<String> i = exportRecords.iterator();
					while (i.hasNext()) {
						String line = i.next();
						line = line.replace(", ", " ");
						String yearExport = line.split(",")[1];
						String commCodeExport = line.split(",")[2];
//						String flowE = line.split(",")[4];

						// Join on year and commCode
						if (year.equals(yearExport) && commCode.equals(commCodeExport)) {
							context.write(null, new Text(recordStr+" : "+ line));
						}
					}
				}
			}
			}
		}


	@Override
	public int run(final String[] args) throws Exception {
		Configuration conf = new Configuration();
		Job job = new Job(conf, "Replicated Join");
		job.setJarByClass(CommodityFlowFinder.class);

		job.setMapperClass(CommodityMapper.class);
		job.setNumReduceTasks(0);

		TextInputFormat.setInputPaths(job, new Path(args[0]));
		TextOutputFormat.setOutputPath(job, new Path(args[1]));

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
