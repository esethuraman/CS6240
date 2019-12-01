package hbase;

import models.CommodityInfo;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;

public class PopulateHBaseMapper extends  Mapper<Object, Text, Text, Text> {

    HbaseDao hbaseDao;

    public PopulateHBaseMapper() throws IOException {
        hbaseDao = new HbaseDao();
    }

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

        System.out.println("BEFORE TRY IN MAPPER....");
        try {
            CommodityInfo commodityInfo = getCommodityInfo(contents);
            System.out.println("MAPPER CALL INVOKED........");
//            Ignoring noise and processing only valid data
            if (commodityInfo != null) {
                Text mapKey = getMapperEmitKey(commodityInfo);
                Text mapValue = getMapperEmitValue(commodityInfo);
                context.write(mapKey, mapValue);
//                context.write(mapKey, new Text("THIS IS FROM NEW "));

                System.out.println("INVOKED HBASE CALL....");
                hbaseDao.writeData(mapKey.toString(), commodityInfo);
                System.out.println("HBASE CALL FINISHED....");


//                Data expansion.
//                for (int i = 0; i < 2; i++) {
//                    commodityInfo.setWeight(Math.random() * 1000000);
//                    mapKey = getMapperEmitKey(commodityInfo);
//                    mapValue = getMapperEmitValue((commodityInfo));
//                    context.write(mapKey, mapValue);
//                }

            }
        } catch (Exception e) {
//                No quantity type commodities are silently ignored using exception
            System.out.println("EXCEPTION CAUGHT.....");
        }


    }

    /**
     *
     * @param commodityInfo instance that contains all information about the commodity.
     * @return a composite key of the form - (code-year-weight)
     */
    private Text getMapperEmitKey(CommodityInfo commodityInfo) {

        return new Text(String.join("-",
                commodityInfo.getCode(),
                String.valueOf(commodityInfo.getYear()),
                String.valueOf(commodityInfo.getWeight())
        ));
    }

    /**
     *
     * @param commodityInfo instance that contains all information about the commodity.
     * @return a text value where all the desired fields for projetion are included.
     */
    private Text getMapperEmitValue(CommodityInfo commodityInfo) {
        return new Text(String.join("-",
                commodityInfo.getFlow(),
                commodityInfo.getCountry(),
                String.valueOf(commodityInfo.getWeight())));
    }

    /**
     *
     * @param contents raw sequence of contents parsed from the input record.
     * Input record is of the form:
     *       country_or_area,year,comm_code,commodity,flow,trade_usd,weight_kg,quantity_name,quantity,category
     * EXAMPLE:
     *        Afghanistan,2016,10410,"Sheep, live",Export,6088,2339.0,Number of items,51.0,01_live_animals
     * @return a model containing all information of input contents.
     */
    private CommodityInfo getCommodityInfo(String[] contents) {
        CommodityInfo info = null;
//        country_or_area,year,comm_code,commodity,flow,trade_usd,weight_kg,quantity_name,quantity,category
//        Afghanistan,2016,10410,"Sheep, live",Export,6088,2339.0,Number of items,51.0,01_live_animals

        if((contents.length > 0) && (contents[0].length() > 0)) {
            info = new CommodityInfo();
            info.setCountry(contents[0]);
            info.setYear(Integer.parseInt(contents[1]));
            info.setCode(contents[2]);
            info.setCommodity(contents[3]);
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
