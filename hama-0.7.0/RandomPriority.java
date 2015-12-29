import java.io.IOException;
import java.util.Iterator;
import java.util.Random;
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

/**
 * There are 3 kinds of nodes.
 *      1) S:The MIS being constructed. Starts empty and grows in iterations.
 *      2) NotInS: Vertices that have at least one edge to a vertex in S and as
 *         a result cannot be in S.
 *      3) Unknown: Vertices that do not have an edge to any vertex in S
 *         but are not yet in S.
 */
public class RandomPriority {

    public static class MISVertex extends
            Vertex<LongWritable, NullWritable, LongWritable>{

        @Override
        public void compute(Iterable<LongWritable> msgIterable) throws IOException {
            if(isHalted())
              return;
            Iterator<LongWritable> messages = msgIterable.iterator();

            if (getSuperstepCount() == 0) {
                // All nodes are initially in Unknown set.
                Random rand = new Random();
                int rand_int = rand.nextInt(Integer.MAX_VALUE) + 1;
                if(rand_int % 2 == 0)
                  rand_int++;
                LongWritable random = new LongWritable(rand_int);
                setValue(random);

                // sends its own random value to all its neighbors
                sendMessageToNeighbors(getValue());
            } else {

                 // if the node is already in NotInS or S, keeps inactive.
                if(getValue().get() == -2 || getValue().get() == -1) {
                    voteToHalt();
                } else {

                    // recMsg indicates whether node has received message.
                    // There are 2 messages in this algorithm:
                    // (1) The node in Unknown set sends message to its neighbors.
                    // (2) Node in S sends -2 to its neighbors in order to notify its neighbors to go to NotInS.
                    boolean revMsg = false;
                    while (messages.hasNext()) {
                        revMsg = true;

                        if(getValue().get() % 2 == 0)
                          setValue(new LongWritable(getValue().get() + 1));

                        long msg = messages.next().get();
                        if (msg == -2) {
                            // one of its neighbors is already in S, so this node has to go to NotInS and become inactive
                            setValue(new LongWritable(-2));
                            voteToHalt();
                            return;
                        } else if (msg < getValue().get()) {
                            // if this node receives a random value that is smaller than its own random value, then this node cannot go to S. It has to keep staying in Unknown
                            return;
                        }

                    }
                    if (revMsg) {
                        // its random value is the smallest among all its neighbors, so it can go to S.
                        setValue(new LongWritable(-1));
                        // sends -2 to notify its neighbors to go to NotInS.
                        sendMessageToNeighbors(new LongWritable(-2));
                        voteToHalt(); //becomes inactive.
                    } else {
                        if (getValue().get() % 2 == 0){
                          // All its neighbors are inactive, so it can go to S.
                          // And it sends -2 to its neighbors in order to notify its neighbors to go to NotInS.
                          setValue(new LongWritable(-1));
                          sendMessageToNeighbors(new LongWritable(-2));
                          voteToHalt();
                        }
                        else {

                          Random rand = new Random();
                          int rand_int = rand.nextInt(Integer.MAX_VALUE) + 1;
                          if(rand_int % 2 == 1)
                            rand_int++;
                          LongWritable random = new LongWritable(rand_int);
                          setValue(random);

                          // the nodes that are in Unknown will send a new random value to its neighbors.
                          sendMessageToNeighbors(getValue());
                        }
                    }
                }
            }
        } // end of compute()
    }

    // read input file
    public static class MISTextReader extends
        VertexInputReader<LongWritable, Text, LongWritable, NullWritable, LongWritable> {

        @Override
        public boolean parseVertex(LongWritable key, Text value,
                Vertex<LongWritable, NullWritable, LongWritable> vertex)
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
        GraphJob pageJob = new GraphJob(conf, RandomPriority.class);
        pageJob.setJobName("Find a MIS");

        pageJob.setMaxIteration(10000);
        pageJob.setVertexClass(MISVertex.class);
        pageJob.setInputPath(new Path(args[0]));
        pageJob.setOutputPath(new Path(args[1]));

        pageJob.setVertexIDClass(LongWritable.class);
        pageJob.setVertexValueClass(LongWritable.class);
        pageJob.setEdgeValueClass(NullWritable.class);

        pageJob.setInputKeyClass(LongWritable.class);
        pageJob.setInputValueClass(Text.class);
        pageJob.setInputFormat(TextInputFormat.class);
        pageJob.setVertexInputReaderClass(MISTextReader.class);
        pageJob.setPartitioner(HashPartitioner.class);
        pageJob.setOutputFormat(TextOutputFormat.class);
        pageJob.setOutputKeyClass(Text.class);
        pageJob.setOutputValueClass(LongWritable.class);
        pageJob.waitForCompletion(true);
    }
}
