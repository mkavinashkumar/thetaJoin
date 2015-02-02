import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Iterator;
import java.util.Random;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.util.*;

public class SelfJoin {
	public static class SelfJoinMap extends MapReduceBase implements
			Mapper<LongWritable, Text, Text, Text> {
		private static Text reducerNum = new Text();
		private static Text tuple = new Text();

		public void map(LongWritable key, Text value,
				OutputCollector<Text, Text> output, Reporter reporter)
				throws IOException {
			String line[] = value.toString().split(",");

			String ymdh = line[0];
			String userID = line[1];
			String queryString = line[19];
			int click = Integer.parseInt(line[3]);

			int reducers[][] = new int[25][25];
			int count = 1;
			for (int i = 0; i < 25; i++) {
				for (int j = 0; j < 25; j++) {
					reducers[i][j] = count;
					count++;
				}
			}
			// Size of the reducer matrix
			int size = 24;
			int rowNum, colNum;
			if (click == 1) {
				// Random Number generation
				Random rand = new Random();
				rowNum = rand.nextInt((size - 0) + 1) + 0;
				colNum = rand.nextInt((size - 0) + 1) + 0;
				tuple.set("a," + userID + ',' + ymdh + ',' + queryString);
				for (int i = 0; i < 25; i++) {
					reducerNum.set(Integer.toString(reducers[rowNum][i]));
					output.collect(reducerNum, tuple);
				}
				tuple.set("b," + userID + ',' + ymdh + ',' + queryString);
				for (int i = 0; i < 25; i++) {
					reducerNum.set(Integer.toString(reducers[i][colNum]));
					output.collect(reducerNum, tuple);
				}
			}
		}
	}

	public static class SelfJoinReduce extends MapReduceBase implements
			Reducer<Text, Text, NullWritable, Text> {

		private static Text record = new Text();

		public void reduce(Text key, Iterator<Text> values,
				OutputCollector<NullWritable, Text> output, Reporter reporter)
				throws IOException {
			// Performing a self join on the tuples with the condition
			// Calculating Join only for row and column tuples
			// in order
			// to avoid duplicates
			ArrayList<String> a = new ArrayList<String>();
			ArrayList<String> b = new ArrayList<String>();
			while (values.hasNext()) {
				String tuple = values.next().toString();
				if (tuple.charAt(0) == 'a') {
					a.add(tuple);
				} else {
					b.add(tuple);
				}
			}
			SimpleDateFormat format = new SimpleDateFormat(
					"yyyy-MM-dd HH:mm:ss");
			Date dateA = null;
			Date dateB = null;
			long diffSeconds = 0;
			for (int i = 0; i < a.size(); i++) {

				String tupleA[] = a.get(i).split(",");
				for (int j = 0; j < b.size(); j++) {
					String tupleB[] = b.get(j).split(",");

					if ((!tupleA[1].equals(tupleB[1]))) {
						// System.out.println(tupleA[0] + ',' + tupleB[0]);
						// Calculating difference in times
						try {
							dateA = format.parse(tupleA[2]);
							dateB = format.parse(tupleB[2]);
							// System.out.println(dateA);
							// in milliseconds
							long diff = dateB.getTime() - dateA.getTime();
							diffSeconds = diff / 1000;

						} catch (Exception e) {
							e.printStackTrace();
						}

						if (Math.abs(diffSeconds) < 2) {
							record.set(tupleA[2] + ',' + tupleA[3] + ','
									+ tupleB[3]);
							output.collect(NullWritable.get(), record);
						}
					}

				}
			}
		}
	}

	public static void main(String[] args) throws Exception {
		JobConf conf = new JobConf(SelfJoin.class);
		conf.setJobName("Self Join using Map-Reduce");
		conf.setOutputKeyClass(Text.class);
		conf.setOutputValueClass(Text.class);
		conf.setMapperClass(SelfJoinMap.class);
		conf.setReducerClass(SelfJoinReduce.class);
		Path outputPath = new Path(args[1]);
		FileInputFormat.setInputPaths(conf, new Path(args[0]));
		outputPath.getFileSystem(conf).delete(outputPath, true);
		FileOutputFormat.setOutputPath(conf, new Path(args[1]));
		JobClient.runJob(conf);
	}
}