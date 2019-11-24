import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class CommodityIdComparator extends WritableComparator {

    public CommodityIdComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        try {
            String id1 = ((Text) first).toString().split("-")[0];
            String id2 = ((Text) second).toString().split("-")[0];
            return id1.compareTo(id2);
        } catch (NullPointerException ex) {
            System.out.println("----------------> " + first);
        }
        return 1;
    }
}
