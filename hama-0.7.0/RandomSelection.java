import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
import java.lang.Math;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hama.HamaConfiguration;
import org.apache.hama.bsp.HashPartitioner;
import org.apache.hama.bsp.TextInputFormat;
import org.apache.hama.bsp.TextOutputFormat;
import org.apache.hama.graph.Edge;
import org.apache.hama.graph.GraphJob;
import org.apache.hama.graph.Vertex;
import org.apache.hama.graph.VertexInputReader;

public class RandomSelection {
    public static class MISVertex extends Vertex<LongWritable, NullWritable, Text>{

        public Text setDeg(Text t, long deg) {
          String [] strArr = t.toString().split(":");
          t.set(String.valueOf(deg) + ":" + strArr[1]);
          return t;
        }

        public Text setIsMark(Text t, boolean isMark) {
          String [] strArr = t.toString().split(":");
          t.set(strArr[0] + ":" + String.valueOf(isMark));
          return t;
        }

        public long getDeg(Text t){
          String [] strArr = t.toString().split(":");
          return Long.parseLong(strArr[0]);
        }

        public boolean getIsMark(Text t){
          String [] strArr = t.toString().split(":");
          return Boolean.parseBoolean(strArr[1]);
        }

        public void randomMark(Text t) throws IOException {
          long deg = getEdges().size();
          t = setDeg(t, deg);

          Random rand = new Random();
          int  random = rand.nextInt((int)(2*deg)) + 1;
          if (random == 1) {
            setValue(setIsMark(t, true));
            sendMessageToNeighbors(getValue());
          }
          else {
            setValue(setIsMark(t, false));
            sendMessageToNeighbors(new Text("0:false"));
          }
        }

        public void chosenIntoMIS() throws IOException {
            setValue(new Text("-1:false"));
            sendMessageToNeighbors(new Text("-2:false"));
            voteToHalt();
        }

        public void neighborOfMIS() throws IOException {
            setValue(new Text("-2:false"));
            voteToHalt();
        }

        @Override
        public void compute(Iterable<Text> msgIterable) throws IOException {

            if(isHalted()) { // Once it is stopped it will always remain in S or N.
              voteToHalt();
              return;
            }

            if(getSuperstepCount() == 0) { // Initialize.
              randomMark(new Text("0:false"));
              return;
            }

            Iterator<Text> messages = msgIterable.iterator();

            if(getSuperstepCount() % 2 == 0) { // Randomly mark itself and send to neighbors at even rounds.
              randomMark(new Text(getValue()));
              if(messages.hasNext()) // If receives -2 message then go into N.
                neighborOfMIS();
            }
            else { // Receive message and decide at odd rounds.
              if(getIsMark(getValue())){ // It is marked in the previous round.
                while (messages.hasNext()) {
                  // Unmark itself if it is marked and receives a message from a marked higher degree neighbor.
                  if (getDeg(messages.next()) >= getDeg(getValue())) {
                    setValue(setIsMark(getValue(), false));
                    return;
                  }
                }
                // If it is still marked, there are no higher degree neighbor that is also marked.
                chosenIntoMIS();
              }
              else if(!messages.hasNext()){ // It is not marked in the previous round.
                // Did not receive message from neighbors, they are all in S or N.
                chosenIntoMIS();
              }
            }
        } // end of compute()
    }

    public static class MISTextReader extends
        VertexInputReader<LongWritable, Text, LongWritable, NullWritable, Text> {

        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<LongWritable, NullWritable, Text> vertex)
                throws Exception {
            String[] split = value.toString().split(", ");
            for (int i = 0; i < split.length; i++) {
                if (i == 0) {
                    vertex.setVertexID(new LongWritable(Long.parseLong(split[i])));
                } else {
                    vertex.addEdge(new Edge<LongWritable, NullWritable>(
                            new LongWritable(Long.parseLong(split[i])), null));
                }
            }
            return true;
        }
    }

    public static void main(String[] args) throws IOException,
            InterruptedException, ClassNotFoundException {
        if (args.length < 2) {
            System.err.println("Usage: <input> <output>");
            System.exit(-1);
        }

        HamaConfiguration conf = new HamaConfiguration(new Configuration());
        GraphJob pageJob = new GraphJob(conf, RandomSelection.class);
        pageJob.setJobName("Find a MIS");

        pageJob.setMaxIteration(10000);
        pageJob.setVertexClass(MISVertex.class);
        pageJob.setInputPath(new Path(args[0]));
        pageJob.setOutputPath(new Path(args[1]));

        pageJob.setVertexIDClass(LongWritable.class);
        pageJob.setVertexValueClass(Text.class);
        pageJob.setEdgeValueClass(NullWritable.class);

        pageJob.setInputKeyClass(LongWritable.class);
        pageJob.setInputValueClass(Text.class);
        pageJob.setInputFormat(TextInputFormat.class);
        pageJob.setVertexInputReaderClass(MISTextReader.class);
        pageJob.setPartitioner(HashPartitioner.class);
        pageJob.setOutputFormat(TextOutputFormat.class);
        pageJob.setOutputKeyClass(Text.class);
        pageJob.setOutputValueClass(Text.class);
        pageJob.waitForCompletion(true);
    }
}
