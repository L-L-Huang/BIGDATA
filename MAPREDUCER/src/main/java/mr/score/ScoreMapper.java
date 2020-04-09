package mr.score;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import java.io.IOException;
import java.util.StringTokenizer;

public class ScoreMapper extends Mapper<LongWritable, Text, Text, IntWritable> {

    @Override
    protected void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
        String line = new String(value.getBytes(),0,value.getLength(),"UTF-8");
        StringTokenizer itr = new StringTokenizer(line);
        String strName = itr.nextToken();
        String strScore = itr.nextToken();
        Text name = new Text(strName);
        int scoreInt = Integer.valueOf(strScore);
        context.write(name, new IntWritable(scoreInt));
    }
}
