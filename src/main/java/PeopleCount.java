import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;
import java.util.StringTokenizer;

public class PeopleCount {
    public PeopleCount() {
    }
    //主函数
    public static void main(String[] args) throws Exception {
        //创建Configuration对象,用于读取配置文件
        Configuration conf = new Configuration();
        //getRemainingArgs()返回除了程序名以外的参数,即输入输出路径,如果参数个数小于2,则打印提示信息并退出程序
        String[] otherArgs = (new GenericOptionsParser(conf, args)).getRemainingArgs();
        if (otherArgs.length < 2) {
            System.err.println("Usage: hadoop jar peoplecount.jar <in> [<in>...] <out>");
            System.exit(2);
        }

        //创建Job对象,用于封装本次作业的相关信息
        Job job = Job.getInstance(conf, "people count");
        //设置主类
        job.setJarByClass(PeopleCount.class);
        //设置Mapper类
        job.setMapperClass(PeopleCountTokenizerMapper.class);
        //设置Combiner类
        job.setCombinerClass(PeopleCountReducer.class);
        //设置Reducer类
        job.setReducerClass(PeopleCountReducer.class);
        //设置输出键类型
        job.setOutputKeyClass(Text.class);
        //设置输出值类型
        job.setOutputValueClass(IntWritable.class);

        //循环遍历输入路径,将输入路径添加到FileInputFormat对象中
        for (int i = 0; i < otherArgs.length - 1; ++i) {
            FileInputFormat.addInputPath(job, new Path(otherArgs[i]));
        }

        //设置输出路径,将输出路径添加到FileOutputFormat对象中
        FileOutputFormat.setOutputPath(job, new Path(otherArgs[otherArgs.length - 1]));
        //等待作业完成,退出程序.waitForCompletion()返回true表示作业成功完成,否则表示作业失败
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }

    //统计人数,继承Mapper类,设置输入类型<Object, Text>和输出类型<Text, IntWritable>
    public static class PeopleCountTokenizerMapper extends Mapper<Object, Text, Text, IntWritable> {
        private static final IntWritable one = new IntWritable(1);
        private final Text author = new Text();
        private Text survival = new Text();
        private Text die = new Text();
        private Text sex = new Text();

        public PeopleCountTokenizerMapper() {
        }

        public void map(Object key, Text value, Mapper<Object, Text, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            //使用逗号分隔符分割字符串
            StringTokenizer itr = new StringTokenizer(value.toString(), ",");
            //输出作者信息
            this.author.set("Author:\nChenYuwen\nLiuShun\nHeZhiwei\n2023-4-19\n总计算次数:");
            context.write(this.author, one);
            //循环遍历分割后的字符串,hasMoreTokens()判断是否还有分割后的字符串
            while (itr.hasMoreTokens()) {
                //获取每个分割后的字符串,trim()去除字符串两端的空格
                String token = itr.nextToken().trim();
                //将字符串分割为数组
                String[] values = value.toString().split(",");
                //判断是否存活，equalsIgnoreCase()忽略大小写
                if (token.equalsIgnoreCase("Yes")) {
                    this.survival.set("存活");
                    context.write(this.survival, one);
                    //判断性别，values[4]为性别字段
                    if (values[4].equalsIgnoreCase("Male")) {
                        this.sex.set("男性存活数");
                        context.write(this.sex, one);
                    } else if (values[4].equalsIgnoreCase("Female")) {
                        this.sex.set("女性存活数");
                        context.write(this.sex, one);
                    }
                    //判断是否死亡，equalsIgnoreCase()忽略大小写
                } else if (token.equalsIgnoreCase("No")) {
                    this.die.set("死亡");
                    context.write(this.die, one);
                    //判断性别，values[4]为性别字段
                    if (values[4].equalsIgnoreCase("Male")) {
                        this.sex.set("男性死亡数");
                        context.write(this.sex, one);
                    } else if (values[4].equalsIgnoreCase("Female")) {
                        this.sex.set("女性死亡数");
                        context.write(this.sex, one);
                    }
                }
                //判断性别
                if (token.equalsIgnoreCase("Male")) {
                    this.sex.set("男性总数");
                    context.write(this.sex, one);
                } else if (token.equalsIgnoreCase("Female")) {
                    this.sex.set("女性总数");
                    context.write(this.sex, one);
                }
            }
        }
    }

    //统计人数,继承Reducer类,设置输入类型<Text, IntWritable>和输出类型<Text, IntWritable>
    public static class PeopleCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {
        //设置输出变量result,类型为IntWritable,初始值为0,用于存储最终结果
        private IntWritable result = new IntWritable();

        public PeopleCountReducer() {
        }


        //这个函数接收三个参数：一个Text类型的key，一个IterableWritable>类型的values和一个Reducer, IntWritable, Text, IntWritable>.Context类型的context。
        //其中，key是输入的键，values是与该键相关的值的集合，context是Reducer的上下文对象，用于输出结果。
        //在函数体内，使用for循环遍历values集合，将集合中的每个元素的值累加到sum变量中，最后将结果写入context中。
        public void reduce(Text key, Iterable<IntWritable> values, Reducer<Text, IntWritable, Text, IntWritable>.Context context) throws IOException, InterruptedException {
            int sum = 0;
            for (IntWritable value : values) {
                sum += value.get();
            }
            this.result.set(sum);
            context.write(key, this.result);
        }
    }
}