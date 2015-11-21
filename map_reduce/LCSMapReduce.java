/* Find LCS in top K pairs among N docs */
import java.io.*;
import java.util.*;
import java.lang.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.*;
import org.apache.hadoop.util.*;
import org.apache.hadoop.mapred.*;
import org.apache.hadoop.mapred.jobcontrol.Job;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.TextOutputFormat;

public class LCScombined {

  private static int K;

  public static void main(String[] args) throws Exception {

    Scanner in = new Scanner(System.in);
    System.out.println("\n Enter K (for selecting top K pairs) ");
    K = in.nextInt();

    JobConf conf1 = new JobConf(LCScombined.class);

    conf1.setJobName("lcs_j1");

    Job job1 = new Job(conf1);
    conf1.setOutputKeyClass(Text.class);
    conf1.setOutputValueClass(IntWritable.class);

    conf1.setMapOutputValueClass(Text.class);
    conf1.setMapOutputKeyClass(IntWritable.class);

    conf1.setMapperClass(Map1.class);
    conf1.setReducerClass(Reduce1.class);

    conf1.setInputFormat(TextInputFormat.class);
    conf1.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.addInputPath(conf1, new Path(args[0]));
    FileOutputFormat.setOutputPath(conf1, new Path("lcs_length"));

    JobClient.runJob(conf1);

    //Reverse Sort the file obtained
    ArrayList<String> arr = new ArrayList<String>();
    String str;
    int size = 0;
    BufferedReader br = new BufferedReader(new FileReader("lcs_length/part-00000"));
    while((str = br.readLine()) != null) {
      size++;
      arr.add(str);
    }

    String[] a = new String[size];
    int i=0;

    for(String s:arr)
    {	a[i] = s; i++; }

    QuickSort(a,0,size-1);
    br.close();

    BufferedWriter bw = new BufferedWriter(new FileWriter("lcs_length2.txt"));
    System.out.println("Sorted Strings : \n ");
    for(i=0; i<size && i < K; i++) {
      if(a[i] != null) {
        bw.write(a[i]);
      }
      bw.write("\n");
    }
    bw.close();

    //Job 2 to select top K subseq lengths and find intersection with K pairs
    JobConf conf2 = new JobConf(LCScombined.class);

    conf2.setJobName("lcs_j2");
    Job job = new Job(conf2);

    conf2.setOutputValueClass(Text.class);
    conf2.setOutputKeyClass(IntWritable.class);

    conf2.setMapOutputValueClass(Text.class);
    conf2.setMapOutputKeyClass(IntWritable.class);

    conf2.setMapperClass(Map2.class);
    conf2.setReducerClass(Reduce2.class);

    conf2.setInputFormat(TextInputFormat.class);
    conf2.setOutputFormat(TextOutputFormat.class);

    FileInputFormat.addInputPath(conf2, new Path("lcs_length2.txt"));
    FileOutputFormat.setOutputPath(conf2, new Path(args[1]));

    JobClient.runJob(conf2);
  }

  // first Mapper - maps file pairs contained in lcs_input to unique id obtained from file
  public static class Map1 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

    public void map(LongWritable key, Text value, OutputCollector<IntWritable,Text> out, Reporter reporter)
      throws IOException {

      String line = value.toString();
      StringTokenizer tokenizer = new StringTokenizer(line);
      int id;

      if(tokenizer.hasMoreTokens()) {
        id = Integer.parseInt(tokenizer.nextToken());
        String filepair = tokenizer.nextToken() + " " + tokenizer.nextToken();
        out.collect(new IntWritable(id), new Text(filepair));
      }
    }
  }

  // reducer emits file pair along with coreponding length of lcs
  public static class Reduce1 extends MapReduceBase implements Reducer<IntWritable, Text, Text, IntWritable> {

    public void reduce(IntWritable key, Iterator<Text> values, OutputCollector<Text,IntWritable> out, Reporter reporter)
      throws IOException {

      String s1, s2;
      int length = 0;
      while (values.hasNext()) {
        String files = values.next().toString();
        String[] names = files.split(" ");
        String str1 = getFileContents(names[0]);
        String str2 = getFileContents(names[1]);
        length = LCSlength(str1, str2);
        out.collect(new Text(names[0]+" "+names[1]), new IntWritable(length));
      }
    }
  }

  //second mapper calculates lcs for the individual file pair
  public static class Map2 extends MapReduceBase implements Mapper<LongWritable, Text, IntWritable, Text> {

    private final static IntWritable one = new IntWritable(1);
    //assuming that only top K entries are given to map and maps all records to a single reducer

    public void map( LongWritable key,Text values, OutputCollector<IntWritable,Text> out, Reporter reporter)
    throws IOException {
      String files = values.toString();
      String[] fields = files.split("\t");
      String[] s1 = fields[0].split(" ");
      String str1 = getFileContents(s1[0]);
      String str2 = getFileContents(s1[1]);
      String lcs = computeLCS(str1, str2);

      out.collect(one, new Text(lcs));
    }
  }

  //get intersection of lcs of all docs
  public static class Reduce2 extends MapReduceBase implements Reducer<IntWritable, Text, IntWritable,Text> {

    private static Text t = new Text();

    public void reduce(IntWritable key, Iterator<Text> values,OutputCollector<IntWritable,Text> out, Reporter reporter)
      throws IOException {

      String cur = null;
      String next = null ;
      String str = null;

      while(values.hasNext()) {
        if(cur == null) {
          cur = values.next().toString();
          if(values.hasNext()) {
            next = values.next().toString();
            str = computeLCS(cur,next);
          }
        }
        else {
          cur = values.next().toString();
          str = computeLCS(cur,str);
        }
      }

      if(str == null) {
        t = new Text(" No subsequence!");
      } else {
        t = new Text(str);
      }
      out.collect(key,t);
    }
  }

  // to reverse sort records obtained from first reducer
  public static void QuickSort(String arr[], int lo, int hi) {
    if(lo < hi)
    {
      int index = randompart(arr,lo,hi);
      QuickSort(arr, lo , index-1);
      QuickSort(arr, index+1, hi);
    }
  }

  public static int randompart(String arr[], int lo, int hi) {
    int key = (int) Math.random()*(hi - lo) + lo;
    swap(arr,hi,key);
    return partition(arr, lo, hi);
  }

  public static void swap(String a[], int i, int j) {
    String tmp = a[i];
    a[i] = a[j];
    a[j] = tmp;
  }

  public static int partition(String arr[], int lo, int hi) {
    String key = arr[hi];
    int i = lo-1;
    int n1, n2;
    for(int j = lo; j <= hi-1; j++) {
      if(arr[j] != null) {
        String[] files = key.split("\t");
        n1 = Integer.parseInt(files[0]);
        n2 = Integer.parseInt(files[1]);
        if(n1 >= n2) {
          i++;
          swap(arr,i,j);
        }
      }
    }
    swap(arr,i+1,hi);
    return i+1;
  }

  public static String computeLCS(String x, String y) throws IOException {
    int M,N;
    M = N = 0;
    int[][] opt = new int[M+1][N+1];
    // compute length of LCS and all subproblems via dynamic programming
    for (int i = 0; i <= M; i++) {
      for (int j = 0; j <= N; j++) {
          if(i==0 || j== 0) {
              opt[i][j] = 0;
          } else if (x.charAt(i-1) == y.charAt(j-1)) {
              opt[i][j] = opt[i-1][j-1] + 1;
          } else {
              opt[i][j] = Math.max(opt[i-1][j], opt[i][j-1]);
          }
      }
    }

    // recover LCS itself
    int i = 0, j = 0;
    StringBuilder sb = new StringBuilder();

    while(i < M && j < N) {
      if (x.charAt(i) == y.charAt(j)) {
        sb.append(x.charAt(i));
        i++;
        j++;
      }
      else if (opt[i+1][j] >= opt[i][j+1]) {
        i++;
      } else j++;
    }
    return sb.toString();
  }

  //returns length of longest common subsequence for two strings x and y
  public static int LCSlength(String x, String y) throws IOException{
      int M,N;
      M = N = 0;
      int[][] opt = new int[M+1][N+1];
      // compute length of LCS and all subproblems via dynamic programming
      for (int i = 0; i <= M; i++) {
        for (int j = 0; j <= N; j++) {
            if(i==0 || j== 0) {
                opt[i][j] = 0;
            } else if (x.charAt(i-1) == y.charAt(j-1)) {
                opt[i][j] = opt[i-1][j-1] + 1;
            } else {
                opt[i][j] = Math.max(opt[i-1][j], opt[i][j-1]);
            }
        }
      }
      return opt[M][N];
  }

  public static String getFileContents(String filename) {
    String str = null;
    StringBuilder sb = new StringBuilder();
    String ls = System.getProperty("line.separator");
    BufferedReader br = new BufferedReader(new FileReader(filename));
    while((str = br.readLine()) != null) {
      M += str.length();
      sb.append(str);
      sb.append(ls);
    }
    br.close();
    return sb.toString();			//get contents of file1 into String x
  }
}
