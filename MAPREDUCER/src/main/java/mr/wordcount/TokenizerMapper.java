package mr.wordcount;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.StringTokenizer;

public class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
    private static final IntWritable one = new IntWritable(1);
    private Text word = new Text();
    public void map(Object key,Text value,Context context) throws IOException, InterruptedException {
        //默认根据空格、制表符、换行符、回车符分隔字符串
        StringTokenizer itr = new StringTokenizer(value.toString());
        //循环输出每个单词与数量
        while(itr.hasMoreTokens()){
            this.word.set(itr.nextToken());
            //输出单词与数量
            context.write(this.word,one);
        }
    }
}
