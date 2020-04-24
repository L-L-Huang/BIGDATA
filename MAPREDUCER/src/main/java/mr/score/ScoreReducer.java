package mr.score;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;
import java.io.IOException;
import java.util.Iterator;

public class ScoreReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException {
        int sum = 0;
        int count = 0;
        Iterator<IntWritable> iterator = values.iterator();
        while (iterator.hasNext()){
            sum += iterator.next().get();
            count++;
        }
        int average = sum / count;
        context.write(key, new IntWritable(average));
    }
}
