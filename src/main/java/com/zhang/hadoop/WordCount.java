package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


import java.io.IOException;
import java.util.StringTokenizer;

public class WordCount {
    //编写TokenizerMapper类继承Mapper类
    public static class TokenizerMapper extends Mapper<Object,Text,Text, IntWritable>{
        //定义变量one值设置为1，为每个单词定义value为1
        public static final IntWritable one=new IntWritable(1);
        private Text word=new Text();
        //编写map函数，其中输入参数为value（即为单词），输出参数为context
        public void map(Object key, Text values, Mapper.Context context) throws IOException, InterruptedException {
            StringTokenizer str=new StringTokenizer(values.toString());
            while(str.hasMoreTokens()){
                word.set(str.nextToken());
                context.write(word,one);
            }
        }
    }
    //定义IntSumReducer继承Reducer
    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
        private IntWritable result=new IntWritable();
        //定义reduce方法
        public void reduce(Text key,Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
            //遍历，将统计各个单词的总个数
            int sum=0;
            for (IntWritable val:values) {
                sum+=val.get();
            }
            result.set(sum);
            context.write(key,result);
        }
    }
    //编写主函数
    public static void main(String[] args) throws Exception{
        Configuration conf=new Configuration();

        Job job= Job.getInstance(conf,"wordCount");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        //添加文件的输入路径
        FileInputFormat.addInputPath(job, new Path(args[0]));
        //添加文件的输出路径
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true)?0:1);
    }
}
