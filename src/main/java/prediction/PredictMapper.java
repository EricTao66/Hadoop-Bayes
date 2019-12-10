package prediction;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 输入的 key   NullWritable  空占位符
 * 输入的 value Text          文档
 * 输出的 key   TextPair      文档名, 类名
 * 输出的 value TextPair      类名, 文档属于该类的概率
 **/
public class PredictMapper extends Mapper<NullWritable, Text, TextPair, TextPair> {

    TextPair k = new TextPair();
    TextPair v = new TextPair();

    // 获取文件的路径和名称（类名）
    @Override
    protected void setup(Mapper<NullWritable, Text, TextPair, TextPair>.Context context) {
        FileSplit split = (FileSplit) context.getInputSplit();
        Path path = split.getPath();
        k.set(new Text(path.getName()), new Text(path.getParent().getName()));
    }

    @Override
    protected void map(NullWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        for (String classname : Util.CLASS_NAMES) {
            v.set(new Text(classname), new Text(Double.toString(
                    Prediction.conditionalProbabilityForClass(value.toString(), classname))));
            context.write(k, v);
        }
    }
}