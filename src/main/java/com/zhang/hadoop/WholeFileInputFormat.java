package com.zhang.hadoop;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IOUtils;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class WholeFileInputFormat extends
        FileInputFormat<Text, BytesWritable> {
    //set a single .txt file cannot be split.
    @Override
    protected boolean isSplitable(JobContext context, Path filename) {
        return false;
    }
    //the principle of function RecordReader is getting <K,V> from split
    //in this particular case,we use this function to read a single whole .txt file at once,and set the content as V,the name of the file +classname as Key
    @Override
    public RecordReader<Text, BytesWritable> createRecordReader(
            InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException,
            InterruptedException {
        WholeFileRecordReader fileRecordReader =new WholeFileRecordReader();
        fileRecordReader.initialize(inputSplit,taskAttemptContext);
        return fileRecordReader;
    }
}
class WholeFileRecordReader extends RecordReader<Text,BytesWritable>{
    private BytesWritable value=new BytesWritable();
    private Text key=new Text();
    private boolean isProcess=false;
    private FileSplit fileSplit;
    private Configuration configuration;

    @Override
    public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
        fileSplit= (FileSplit) inputSplit;
        configuration=taskAttemptContext.getConfiguration();
    }

    @Override
    public boolean nextKeyValue() throws IOException, InterruptedException {
        if(!isProcess){
            FSDataInputStream inputStream=null;
            FileSystem fileSystem=null;
            try{
                //get value
                byte[] contents =new byte[(int)fileSplit.getLength()];
                //get hadoop FileSystem
                Path path=fileSplit.getPath();
                fileSystem=path.getFileSystem(configuration);
                //open file input steam
                inputStream=fileSystem.open(path);
                //use IOUtil ReadFully method to get .txt file's context to byte[]
                IOUtils.readFully(inputStream,contents,0,contents.length);
                //set value as contents
                value.set(contents,0,contents.length);

                //get key
                String classname=fileSplit.getPath().getParent().getName();
                //get the name of a .txt file
                String filename=fileSplit.getPath().getName();
                //set filenameKey as classname+filename format
                key.set(classname+ "+" +filename);
            }catch(Exception e){
                e.printStackTrace();
            }finally {
                if(inputStream!=null){
                    inputStream.close();
                }
                if(fileSystem!=null){
                    fileSystem.close();
                }

            }isProcess=true;
            return true;
        }
        return false;
    }

    @Override
    public Text getCurrentKey() throws IOException, InterruptedException {
        return key;
    }

    @Override
    public BytesWritable getCurrentValue() throws IOException, InterruptedException {
        return value;
    }

    @Override
    public float getProgress() throws IOException, InterruptedException {
        return isProcess ? 1.0f : 0.0f;
    }

    @Override
    public void close() throws IOException {
    //do nothing.
    }

}
