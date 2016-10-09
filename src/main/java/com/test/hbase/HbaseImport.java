package com.test.hbase;

import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.hbase.mapreduce.TableOutputFormat;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
/**
 * 通过MR批量把hdfs某个目录下的文件导入到hbase中
 * @author Administrator
 *
 */
public class HbaseImport {
	static class BatchImportMapper extends
			Mapper<LongWritable, Text, LongWritable, Text> {
		SimpleDateFormat dateformat1 = new SimpleDateFormat("yyyyMMddHHmmss");
		Text v2 = new Text();

		protected void map(LongWritable key, Text value, Context context)
				throws java.io.IOException, InterruptedException {
			final String[] splited = value.toString().split("\t");
			try {
				final Date date = new Date(Long.parseLong(splited[0].trim()));
				final String dateFormat = dateformat1.format(date);
				String rowKey = splited[1] + ":" + dateFormat;
				v2.set(rowKey + "\t" + value.toString());
				context.write(key, v2);
			} catch (NumberFormatException e) {
				final Counter counter = context.getCounter("BatchImport",
						"ErrorFormat");
				counter.increment(1L);
				System.out.println("出错了" + splited[0] + " " + e.getMessage());
			}
		};
	}

	static class BatchImportReducer extends
			TableReducer<LongWritable, Text, NullWritable> {
		protected void reduce(LongWritable key,
				java.lang.Iterable<Text> values, Context context)
				throws java.io.IOException, InterruptedException {
			for (Text text : values) {
				final String[] splited = text.toString().split("\t");

				final Put put = new Put(Bytes.toBytes(splited[0]));
				put.add(Bytes.toBytes("cf"), Bytes.toBytes("date"),
						Bytes.toBytes(splited[1]));
				// 省略其他字段，调用put.add(....)即可
				context.write(NullWritable.get(), put);
			}
		};
	}

	public static void main(String[] args) throws Exception {
		final Configuration configuration = new Configuration();
		// 设置zookeeper
		configuration.set("hbase.zookeeper.quorum", "hadoop0");//ZK IPorNAME
		// 设置hbase表名称
		configuration.set(TableOutputFormat.OUTPUT_TABLE, "wlan_log");//输出表名
		// 将该值改大，防止hbase超时退出
		configuration.set("dfs.socket.timeout", "180000");

		final Job job = new Job(configuration, "HbaseImport");
		TableMapReduceUtil.addDependencyJars(job);
		job.setJarByClass(HbaseImport.class);

		job.setMapperClass(BatchImportMapper.class);
		job.setReducerClass(BatchImportReducer.class);
		// 设置map的输出，不设置reduce的输出类型
		job.setMapOutputKeyClass(LongWritable.class);
		job.setMapOutputValueClass(Text.class);

		job.setInputFormatClass(TextInputFormat.class);
		// 不再设置输出路径，而是设置输出格式类型
		job.setOutputFormatClass(TableOutputFormat.class);

		FileInputFormat.setInputPaths(job, "hdfs://hadoop0:9000/input");

		job.waitForCompletion(true);
	}
}
