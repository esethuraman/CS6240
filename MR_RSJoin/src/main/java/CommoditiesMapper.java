import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.util.Arrays;

public class CommoditiesMapper
        extends Mapper<Object, Text, Text, Text> {

    /*
    Each input value fed in to mapper is of the form (followerId,userId). Note that followerId
    is in turn an userId.
     */
    @Override
    public void map(final Object key, final Text value, final Context context) throws IOException, InterruptedException {
			/*Tokenizing the input value based on comma. For this dataset, there will always be two
			tokens. Those tokens are followerId and userId.*/

//			id,country_or_area,year,comm_code,commodity,flow,trade_usd,weight_kg,quantity_name,quantity,category
        String[] contents = value
                .toString()
                .replace(", ", " ")
                .split(",");

        try {
            CommodityInfo commodityInfo = getCommodityInfo(contents);
            if (commodityInfo != null) {
                Text mapKey = getMapperEmitKey(commodityInfo);
                Text mapValue = getMapperEmitValue(commodityInfo);
                context.write(mapKey, mapValue);

            }
        } catch (Exception e) {
//                No quantity type commodities are silently ignored using exception
        }


    }

    private Text getMapperEmitKey(CommodityInfo commodityInfo) {

        return new Text(String.join("-",
                commodityInfo.getCode(),
                String.valueOf(commodityInfo.getYear()),
                String.valueOf(commodityInfo.getWeight())
        ));
    }

    private Text getMapperEmitValue(CommodityInfo commodityInfo) {
        return new Text(String.join("-",
                commodityInfo.getFlow(),
                commodityInfo.getCountry(),
                String.valueOf(commodityInfo.getWeight())));
    }

    private CommodityInfo getCommodityInfo(String[] contents) {
        CommodityInfo info = null;

        if((contents.length > 0) && (contents[0].length() > 0)) {
            info = new CommodityInfo();
            info.setCountry(contents[1]);
            info.setYear(Integer.parseInt(contents[2]));
            info.setCode(contents[3]);
            if (contents[4].contains("xport")) {
                info.setFlow("Export");
            } else {
                info.setFlow("Import");
            }
            info.setAmoundInUSD(Long.parseLong(contents[5]));
            info.setWeight(Double.parseDouble(contents[6]));
            info.setQuantityName(contents[7]);
            info.setQuantity(Double.parseDouble(contents[8]));
            info.setCategory(contents[9]);

        }

        return info;
    }

}