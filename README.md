# Trade Recommendation system

TEAM MEMBERS:
1) ELAVAZHAGAN SETHURAMAN
2) HARI PRASATH SIVANANDAM
3) MONICA MISHRA
4) RUSSIA AIYAPPA BENJANDA PEMMAIAH


### MR_HBASE_RS_JOIN 
     In this section, we are populating the Hbase table with input records on which we are running our equi join algorithm.
     
### MR_RS_JOIN
     In this section, we have implemented Reduce-side join in Map reduce. 
     
### MR_REP_JOIN
     In this section, we have implemented replicated join using distributed cache and Hbase in map reduce.

### SPARK_RS_JOIN
     In this section, we have implemented reduce side join in spark.
     
### SPARK_REP_JOIN
     In this section, we have implemented replicated join in spark.

### LOGS
     In this section, we have runs of all the log files for the code implemented above.  

### OUTPUT
     In this section, we have all the output files for the code implemented above.
     
### Project Overview:  
Several countries rely heavily on their counterparts to obtain goods that aren’t easily accessible in their territories. The market of international trade is not new, countries have been exchanging commodities from prehistoric times. As the world is becoming increasingly competitive and informed about any trade choice, it is safe to say that countries would benefit from a system that helps them make better trade decisions by exploiting historical export/import data.  
 
The aim of this project is to build a trade recommendation system using HBase as our distributed dataset along with exercising the equi-join design pattern of Map Reduce. The trade recommendation system would output all the countries that have exported a certain commodity in the past along with the countries that have been known to import the same commodity, however, it is unclear which country, the trade was committed as that information is unavailable in the dataset. This data is grouped based on the calendar year. This would help the user/analyst to understand the data better. And the recommendations with each year are sorted based on their non-increasing order of weights that the commodity was traded. That way, recommendations readily reveal the countries that are directly proportional to each other in terms of exports and imports. 
 
Once the dataset is joined (based on commodity and year) it would be easy to analyze which countries could be a prospective exporter/importer for a wide variety of trade products. The above solution is easily expandable if more data is captured in the future.  
 
We also intend to perform a comparison of the equi-join with different algorithm paradigms (partition + broadcast/hash + shuffle) across Map Reduce and Spark implementation. This would help to understand which technique serves the best to perform equi-join for the problem at hand.  
 
As of now, we are working on setting up HBase and have made some progress different implementations for the join. The code has been tested on a subset of the data, the subset is performed on ‘year’ as the recent years are a more reliable representation of future trends.  
 
We have also played around with the idea of custom partitioning and different key comparator in order to better present the result
