package docCount;

import java.io.IOException;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 输入的 key   Text          类名
 * 输入的 value IntWritable   {1,1,...1}
 * 输出的 key   Text          类名
 * 输出的 value IntWritable   属于该类的文档总个数
 *
 * INPUT:       <Class,{1,1,...1}>
 * OUTPUT:      <Class,TotalCount>
 **/
public class DocCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
            throws IOException, InterruptedException {
        // 1 统计文档总个数
        int sum = 0;
        for (IntWritable count : values) {
            sum += count.get();
        }
        // 2 输出文档总个数
        context.write(key, new IntWritable(sum));
    }
}
