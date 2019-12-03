package cf;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class GroupingComparator extends WritableComparator {

    public GroupingComparator() {
        super(Text.class, true);
    }

    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        try {
            String id1 = ((Text) first).toString().split("-")[0];
            String id2 = ((Text) second).toString().split("-")[0];

            int result = id1.compareTo(id2);
            if (result != 0) {
                return result;
            } else{
                String year1 = ((Text) first).toString().split("-")[1];
                String year2 = ((Text) second).toString().split("-")[1];
                return year2.compareTo(year1);
            }

        } catch (NullPointerException ex) {
            System.out.println("----------------> " + first);
        }
        return 1;
    }
}


