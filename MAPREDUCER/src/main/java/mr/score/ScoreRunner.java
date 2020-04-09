package mr.score;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;

public class ScoreRunner implements Tool {
    private Configuration conf = new Configuration();

    public int run(String[] strings) throws Exception {
        Job job = Job.getInstance(conf,"Score Average");
        job.setJarByClass(ScoreRunner.class);

        job.setMapperClass(ScoreMapper.class);
        job.setReducerClass(ScoreReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job,new Path("/input/"));
        FileOutputFormat.setOutputPath(job,new Path("/output/"));
        return job.waitForCompletion(true) ? 0 : 1;
    }

    public void setConf(Configuration configuration) {
        this.conf = configuration;
    }

    public Configuration getConf() {
        return conf;
    }
}
