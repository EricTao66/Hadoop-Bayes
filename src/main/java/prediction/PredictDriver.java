package prediction;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class PredictDriver {

    public static void main(String[] args)
            throws IOException, URISyntaxException, InterruptedException, ClassNotFoundException {
        args = new String[]{Util.INPUT_PATH, Util.OUTPUT_PATH};
        // 文件处理
        Path input_path = new Path(args[0] + "test/");
        Path output_path = new Path(args[1] + "prediction/");
        Configuration conf = new Configuration();
        FileSystem fs = FileSystem.get(new URI(args[1]), conf);
        if (fs.exists(output_path)) {
            fs.delete(output_path, true);
        }
        Prediction prediction = new Prediction(fs);
        // 1 获取job信息
        Job job = Job.getInstance(conf, "prediction");
        // 2 获取jar包位置
        job.setJarByClass(Prediction.class);
        // 3 关联自定义的mapper和reducer
        job.setMapperClass(PredictMapper.class);
        job.setReducerClass(PredictReducer.class);
        // 4 设置自定义的InputFormat类
        job.setInputFormatClass(PredictTestInputFormat.class);
        // 5 设置map输出数据类型
        job.setMapOutputKeyClass(TextPair.class);
        job.setMapOutputValueClass(TextPair.class);
        // 6 设置最终输出数据类型
        job.setOutputKeyClass(TextPair.class);
        job.setOutputValueClass(Text.class);
        // 7 设置输入和输出文件路径
        Path[] paths = Util.getPaths(input_path, fs);
        for (Path path : paths) {
            FileInputFormat.addInputPath(job, path);
        }
        FileOutputFormat.setOutputPath(job, output_path);
        // 8 提交代码
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}