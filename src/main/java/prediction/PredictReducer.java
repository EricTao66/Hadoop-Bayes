package prediction;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

/**
 * 输入的 key   TextPair      文档名，实际的类别
 * 输入的 value TextPair      {<类名, 文档属于该类的概率>,...}
 * 输出的 key   TextPair      文档名, 实际的类别
 * 输出的 value Text          预测的类别
 *
 * INPUT:       <<docID,ActualClass>,{<Class0,Prob0>,<Class1,Prob1>,...,<Classn,Probn>}>
 * OUTPUT:      <<docID,ActualClass>,PredictClass>
 **/
public class PredictReducer extends Reducer<TextPair, TextPair, TextPair, Text> {

    @Override
    protected void reduce(TextPair key, Iterable<TextPair> values, Context context)
            throws IOException, InterruptedException {
        Text value = new Text();
        double max_prob = Double.NEGATIVE_INFINITY;
        for (TextPair v : values) {
            double cur_prob = Double.parseDouble(v.getSecond().toString());
            if (cur_prob > max_prob) {
                max_prob = cur_prob;
                value.set(v.getFirst());
            }
        }
        context.write(key, value);
    }
}