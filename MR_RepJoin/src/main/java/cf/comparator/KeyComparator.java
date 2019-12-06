package cf.comparator;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;

public class KeyComparator extends WritableComparator {

    public KeyComparator() {
        super(Text.class, true);
    }

    /**
     *  Input objects are of the form: 'commodityCode-year-weight'
     *  First compares commodityCode to be sorted in ascending order. If they are same,
     *  then years are being compared. If year fields are same, then weights are compared.
     */
    @Override
    public int compare(WritableComparable first, WritableComparable second) {
        try {
            String id1 = ((Text) first).toString().split("-")[0];
            String id2 = ((Text) second).toString().split("-")[0];

            Integer year1 = Integer.valueOf(((Text) first).toString().split("-")[1]);
            Integer year2 = Integer.valueOf(((Text) second).toString().split("-")[1]);

            Double weight1 = Double.valueOf(((Text) first).toString().split("-")[2]);
            Double weight2 = Double.valueOf(((Text) second).toString().split("-")[2]);

            int result = id1.compareTo(id2);
            if (result == 0) {
                result = year2.compareTo(year1);
                if (result == 0) {
                    result = weight2.compareTo(weight1);
                }
            }
            return result;
        } catch (Exception e) {

        }
        return 1;
    }
}
