import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class YearComparator extends WritableComparator {

    public YearComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        String year1 = ((Text) first).toString().split("-")[1];
        String year2 = ((Text) second).toString().split("-")[1];
        return year2.compareTo(year1);
    }
}
