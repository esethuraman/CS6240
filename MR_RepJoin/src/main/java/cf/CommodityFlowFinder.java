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
//		private HashSet<String> exportRecords = new HashSet<String>();
		private Map<String, String> exportRecords = new HashMap<String, String>();
		private String recordStr;

			@Override
			public void setup(Context context) throws IOException,
					InterruptedException {
				try {
					Path[] files = DistributedCache.getLocalCacheFiles(context
							.getConfiguration());

					if (files == null || files.length == 0) {
						throw new RuntimeException(
								"Data not set in DistributedCache");
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
							logger.info("map");
							logger.info("here is the line:"+line+":ends");
							String [] lineList = line.split(",");
							String country = lineList[0];
							String year = lineList[1];
							String commCode = lineList[2];
							String commodity = lineList[3];
							String flow = lineList[4];
//							String trade_usd = lineList[5];
							String weight = lineList[6];

							// FILTER_EXPORT
							if (flow.contains("xport")) {
								exportRecords.put(commCode+","+year+","+weight, flow+","+country+","+weight);
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
				String [] recordStrList = recordStr.split(",");

				String country = recordStrList[0];
				String year = recordStrList[1];
				String commCode = recordStrList[2];
//				String commodity = recordStrList[3];
				String flow = recordStrList[4];
				String weight = recordStrList[6];

				// FILTER_IMPORT
				if (flow.equals("Import")) {
					for (Map.Entry<String,String> recordExport : exportRecords.entrySet())   {
						String [] keyList = recordExport.getKey().split(",");
						String [] valueList = recordExport.getValue().split(",");

						logger.info("key");
						logger.info(recordExport.getKey());
						logger.info("value");
						logger.info(recordExport.getValue());

						String commCodeExport = keyList[0];
						String yearExport = keyList[1];
						String weightExport = keyList[2];
						String flowExport = valueList[0];
						String countryExport = valueList[1];
						String weightExportValue = valueList[2];


						logger.info("recordStrList");
						logger.info(commCode+"-"+year+"-"+weight);
						logger.info("keyList");
						logger.info(commCodeExport+"-"+yearExport+"-"+weightExport);
						logger.info("valueList");
						logger.info(flowExport+"-"+countryExport+"-"+weightExportValue);

						// Join on year and commCode
						if (year.equals(yearExport) && commCode.equals(commCodeExport)) {
							context.write(new Text(commCode+"-"+year+"-"+weight),
									new Text(countryExport+"-"+weightExport+"+"+country+"-"+weight));
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
