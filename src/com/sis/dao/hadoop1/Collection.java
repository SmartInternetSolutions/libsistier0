package com.sis.dao.hadoop1;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.svenson.JSONParser;

import com.sis.util.Pair;

/**
 * 
 * 
 * @author CR
 */
public class Collection extends com.sis.dao.Collection {
	protected class DefaultMapper extends MapReduceBase implements Mapper<LongWritable, Text, LongWritable, Text> {
		@Override
		public void map(LongWritable line, Text value, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			// no filters
			if (fieldFilters.isEmpty()) {
				output.collect(line, value);
				
				return;
			}
			
			@SuppressWarnings("unchecked")
			Map<String, Object> document = (Map<String, Object>) JSONParser.defaultJSONParser().parse(value.toString());
			
			for (final String key : fieldFilters.keySet()) {
				Pair<Object[], Integer> pair = fieldFilters.get(key);
				
				if (!document.containsKey(key)) {
					return;
				}
				
				Object obj = document.get(key), values[] = pair.getLeft();
				
				for (int i = 0; i < values.length; i++) {
					if (values[i].toString().equals(obj.toString())) {
						output.collect(line, value);
						return;
					}
				}
			}
		}
	}
	
	protected class DefaultReducer extends MapReduceBase implements Reducer<LongWritable, Text, LongWritable, Text> {
		@Override
		public void reduce(LongWritable key, Iterator<Text> values, OutputCollector<LongWritable, Text> output, Reporter reporter) throws IOException {
			for (; values.hasNext();) {
				output.collect(key, (Text) values.next());
			}
		}
	}
	
	protected void initResource() {
		Resource res = new Resource();
//		res.setTableName(tableName);
//		res.setIdFieldName(idFieldName);

		res.setCluster(idFieldName);
		res.setGroup(tableName);
		
		resource = res;
	}
	
	public Collection() {
		super();
		
		initResource();
	}
	
	protected void loadCollection() {
		JobConf conf = new JobConf(Collection.class);
		conf.setJobName(getTableName() + "/" + getIdFieldName());
		
		conf.setOutputKeyClass(LongWritable.class);
		conf.setOutputValueClass(Text.class);

		conf.setMapperClass(DefaultMapper.class);
		conf.setCombinerClass(DefaultReducer.class);
		conf.setReducerClass(DefaultReducer.class);
		
		conf.setInputFormat(TextInputFormat.class);
		conf.setOutputFormat(TextOutputFormat.class);

		FileInputFormat.setInputPaths(conf, new Path(((Resource) resource).getFileUri().getPath()));
		FileOutputFormat.setOutputPath(conf, new Path("/tmp/mapredtest.txt"));
		
		try {
			JobClient.runJob(conf);
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
