package docCount;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

/**
 * 输入的 key   NullWritable  空占位符
 * 输入的 value BytesWritable 文件流
 * 输出的 key   Text          类名
 * 输出的 value IntWritable   1
 *
 * INPUT:       <Null,File>
 * OUTPUT:      <Class,1>
 **/
public class DocCountMapper extends Mapper<NullWritable, BytesWritable, Text, IntWritable> {

    Text k = new Text();
    IntWritable v = new IntWritable(1);

    @Override
    protected void setup(Mapper<NullWritable, BytesWritable, Text, IntWritable>.Context context) {
        // 获取文件的路径名称（类名）
        FileSplit split = (FileSplit) context.getInputSplit();
        Path path = split.getPath();
        k.set(path.getParent().getName());
    }

    @Override
    protected void map(NullWritable key, BytesWritable value, Context context)
            throws IOException, InterruptedException {
        context.write(k, v);
    }
}
