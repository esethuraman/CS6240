import org.apache.hadoop.mapreduce.Partitioner;

public class CustomPartitioner extends Partitioner {

    @Override
    public int getPartition(Object key, Object value, int numPartitions) {
        String code = String.valueOf(key).split("-")[0];
        String year = String.valueOf(key).split("-")[1];

        return Math.abs((code + "-" + year).hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
}
