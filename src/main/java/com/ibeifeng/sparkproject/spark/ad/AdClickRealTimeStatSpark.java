package com.ibeifeng.sparkproject.spark.ad;

import com.ibeifeng.sparkproject.dao.IAdBlacklistDAO;
import com.ibeifeng.sparkproject.dao.IAdClickTrendDAO;
import com.ibeifeng.sparkproject.dao.IAdStatDAO;
import com.ibeifeng.sparkproject.dao.IAdUserClickCountDAO;
import com.ibeifeng.sparkproject.dao.impl.DAOFactory;
import com.ibeifeng.sparkproject.domain.AdClickTrend;
import com.ibeifeng.sparkproject.domain.AdStat;
import com.ibeifeng.sparkproject.domain.AdUserClickCount;
import com.ibeifeng.sparkproject.util.DateUtils;
import jdk.nashorn.internal.objects.annotations.Function;
import jdk.nashorn.internal.runtime.linker.JavaAdapterFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Created by Administrator on 2017/4/1.
 */
public class AdClickRealTimeStatSpark {
    public static void main(String[] args){
        SparkConf   conf=new SparkConf().setMaster("local[2]").setAppName("AdClickRealTimeStatSpark");
        JavaStreamingContext  jsssc=new JavaStreamingContext(conf,Duration.seconds(5));
        Map<String,String> kafkaParams=new HashMap<String,String>();
        kafkaParams.put("metadata.broker.list",ConfigurationManager.getProperty(Constants.KAFKA_METADATA_BROKER_LIST));
        String  kafkaTopics=ConfigurationManager.getProperty(Constants.KAFKA_TOPICS);
        String[]  kafkaTopicsSplited=kafkaTopics.split(",");
        Set<String>  topics=new HashSet<String>();
        for(String kafkaTopic:kafkaTopicsSplited){
            topics.add(kafkaTopic);
        }
         //    930444933744
        JavaPairInputDStream<String,String>   adRealTimeLogDStream=KafkaUtils.createDirectStream(jssc,String.class,String.class,StringDecoder.class,StringDecoder.class,kafkaParams,topics);
        JavaPairDStream<String,String>  filteredAdRealTimeLongDStream=adRealTimeLogDStream.transformToPair(new Function<JavaPairRDD<String,String>,JavaPairRDD<String,String>>(){
            public JavaPairRDD<String,String>  call(JavaPairRDD<String,String>  rdd){
                  IAdBlacklistDAO adBlacklistDAO=DAOFactory.getAdBlacklistDAO();
                  List<AdBlacklist>   adBlacklists=adBlacklistDAO.findAll();
                  List<Tuple2<Long,Boolean>>  tuples=new ArrayList<Tuple2<Long,Boolean>>();
                  for(AdBlacklist  adBlacklist:adBlacklists){
                      tuples.add(new Tuple2<Long,Boolean>(adBlacklist.getUserid(),true));
                  }
                   JavaSparkContext  sc=new JavaSparkContext(rdd.context());
                   JavaPairRDD<Long,Boolean>  blacklistRDD=sc.parallelizePairs(tuples);
                    JavaPairRDD<Long,Tuple2<String,String>> mappedRDD=rdd.mapToPair(new PairFunction<Tuple2<String,String>,Tuple2<String,String>>(){
                        public  Tuple2<Long,Tuple2<String,String>>  call(Tuple2<String,String> tuple){
                            String log=tuple._2;
                            String[]   logSplited=log.split(" ");
                            long  userid=Long.valueOf(logSplited[3]);
                            return  new Tuple2<Long,Tuple2<String,String>>(userid,tuple);
                        }
                    });
                   JavaPairRDD<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>> joinedRDD=mappedRDD.leftOuterJoin(blacklistRDD);
                    JavaPairRDD<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>> filteredRDD=joinedRDD.filter(new Function<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>,Boolean>(){
                        public Boolean call(Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>> tuple){
                              Optional<Boolean>  optional=tuple._2._2;
                              if(optional.isPresent() && optional.get()){
                                             return  false;
                              }
                                  return  true;
                        }
                    });
                            JavaPairRDD<String,String> resultRDD=filteredRDD.mapToPair(new PairFunction<Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>>,String,String>(){
                                   public  Tuple2<String,String>  call(Tuple2<Long,Tuple2<Tuple2<String,String>,Optional<Boolean>>> tuple){
                                    return  tuple._2._1;
                                }
                            });
/**
 * repartition
 * spark.default.parallelism
 * reduceByKey(,1000)
 * StorageLevel.memory_and_disk_ser_2    冗余副本
 *spark.serializer=org.apache.spark.serializer.KryoSerializer
 * spark.default.parallelism=200
 * spark.streaming.blockInterval=50
 * spark.streaming.receiver.writeAheadLog.enable=true
 * batch.interval   =Duration.seconds(7)
 *volcano iterator  model
 *volcano  iterator  model
 *3exh
 *
 *
 * */
                   return  null;
            }
        });






        JavaPairDStream<String,Long>  dailyUserAdClickDStream=filteredAdRealTimeLogDSstream.mapToPair(new PairFunction<Tuple2<String,String>,String,Long>(){
         public Tuple2<String,Long>  call(Tuple2<String,String> tuple){
                                    String  log=tuple._2;
                                    String[]   logSplited=log.split(" ");
                                    String  timestamp=logSplited[0];
                                    Date date=new Date(Long.valueOf(timestamp));
                                    String datekey= DateUtils.formatDateKey(date);
                                    long  userid=Long.valueOf(logSplited[3]);
                                    long  adid=Long.valueOf(logSplited[4]);
                                    String  key=datekey+"_"+userid+"_"+adid;
                                    return   new Tuple2<String,Long>(key,1);
                          }
                      });
        JavaPairDStream<String,Long>  dailyUserAdClickCountDStream=dailyUserAdClickDStream.reductByKey(new Function2<Long,Long,Long>(){
            public Long  call(Long v1,Long  v2){
                return  v1+v2;
             }
        });
        dailyUserAdClickCountDStream.foreachRDD(new Function<JavaPairRDD<String,Long>  void>(){
            public void  call(JavaPairRDD<String,Long>  rdd){
                rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>(){
                          public void call(Iterator<Tuple2<String,Long>>  iterator){
                              List<AdUserClickCount> adUserClickCounts=new ArrayList<AdUserClickCount>();
                              while(iterator.hasNext()){
                                  Tuple2<String,Long>   tuple=iterator.next();
                                  String[]   keySplited=tuple._1.split("_");
                                  String   date=DateUtils.formatDate(DateUtils.parseDateKey(keySplited[0]));
                                  long  userid=Long.valueOf(keySplited[1]);
                                  long   adid=Long.valueOf(keySplited[2]);
                                  long  clickCount=tuple._2;
                                  AdUserClickCount   adUserClickCount=new AdUserClickCount();
                                  adUserClickCount.setDate(date);
                                  adUserClickCount.setUserid(userid);
                                  adUserClickCount.setAdid(adid);
                                  adUserClickCount.setClickCount(clickCount);
                                  adUserClickCounts.add(adUserClickCount);
                              }
                              IAdUserClickCountDAO adUserClickCountDAO= DAOFactory.getAdUserClickCountDAO();
                              adUserClickCountDAO.updateBatch(adUserClickCounts);
                          }
                });
                   return  null;
            }
        });
        JavaPairDStream<String,Long> mappedDStream==filteredAdRealTimeLongDStream.mapToPair(new PairFunction<Tuple2<String,String>,String,Long>(){
            public  Tuple2<String,Long>  call(Tuple2<String,String> tuple){
              String log=tuple._2;
              String[]  logSplited=log.split(" ");
              String  timestamp=logSplited[0];
              Date  date=new Date(Long.valueOf(timestamp));
              String  datekey=DateUtils.formatDatKey(date);
              String  province=logSplited[1];
              String city=logSplited[2];
              long adid=Long.valueOf(logSplited[4]);
              String key=datekey+"_"+province+"_"+city+"_"+adid;
              return  new  Tuple2<String,Long>(key,1L);
            }
        });
        JavaPairDStream<String,Long> aggregatedDStream=mappedDStream.updateStateByKey(new Function2<List<Long>,Optional<Long>,Optional<Long>>(){
            public Optional<Long> call(List<Long>  values,Optional<Long> optional){
                 long  clickCount=0L;
                 if(optional.isPresent()){
                     clickCount=optional.get();
                 }
                 for(Long  value:values){
                     clickCount+=value;
                 }
                return   Optional.of(clickCount);
            }
        });
        aggregatedDStream.foreachRDD(new Function<JavaPairRDD<String,Long>,Void>(){
              public Void  call(JavaPairRDD<String,Long>  rdd){
                  rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>(){
                      public void  call(Iterator<Tuple2<String,Long>> iterator){
                          List<AdStat>  adStats=new  ArrayList<AdStat>();
                            while(iterator.hasNext()){
                                Tuple2<String,Long> tuple=iterator.next();
                                String[]  keySplited=tuple._1.split("_");
                                String date=keySplited[0];
                                String province=keySplited[1];
                                String  city=keySplited[2];
                                long  adid=Long.valueOf(keySplited[3]);
                                long  clickCount=tuple._2;
                                AdStat adStat=new  AdStat();
                                adStat.setDate(date);
                                adStat.setProvince(province);
                                adStat.setCity(city);
                                adStat.setAdid(adid);
                                adStat.setClickCount(clickCount);
                                adStats.add(adStat);
                            }
                             IAdStatDAO adStatDAO=DAOFactory.getAdStatDAO();
                            adStatDAO.updateBatch(adStats);
                      }
                  });
                  return null;
              }
        });






        jssc.start();
        jssc.awaitTermination();
        jssc.close();
    }
     private static void calculateProvinceTop3Ad(JavaPairDStream<String,Long> adRealTimeStatDStream){
           JavaDStream<Row>  rowsDStream=adRealTimeStatDStream.transform(new Function<JavaPairRDD<String,Long>,JavaRDD<Row>>(){
               public JavaRDD<Row>  call(JavaPairRDD<String,Long>  rdd){
               JavaPairRDD<String,Long>  mappedRDD=rdd.mapToPair(new PairFunction<Tuple2<String,Long>,String,Long>(){
                       public Tuple2<String,Long>  call(Tuple2<String,Long> tuple){
                           String[]  keySplited=tuple._1.split("_");
                           String  date=keySplited[0];
                           String province=keySplited[1];
                           long adid=Long.valueOf(keySplited[3]);
                           long clickCount=tuple._2;
                           String  key=date+"_"+province+"_"+adid;
                           return  new Tuple2<String,Long>(key,clickCount);
                                    }
                                });
              JavaPairRDD<String,Long>    dailyAdClickCountByProvinceRDD=mappedRDD.reduceByKey(new Function2<Long,Long,Long>(){
                   public Long call(Long v1,Long v2){return v1+v2;}
               });
               }
           });
        JavaRDD<Row> rowsRDD=dailyAdClickCountByProvinceRDD.map(new Function<Tuple2<String,Long>,Row>(){
              public Row call(Tuple2<String,Long> tuple){
                  String[] keySplited=tuple._1.split("_");
                  String  dateKey=keySplited[0];
                  String  province=keySplited[1];
                  long adid=Long.valueOf(keySplited[2]);
                  long  clickCount=tuple._2;
                  String date=DateUtils.formatDate(DateUtils.parseDateKey(dateKey));
                  return  RowFactory.create(date,province,adid,clickCount);
              }
        });
        StructType schema=DataTypes.createStructType(Arrays.asList(
                DataTypes.createStructField("date",DataTypes.StringType,true),
                DataTypes.createStructField("province",DataTypes.StringType,true),
                DataTypes.createStructField("ad_id",DataTypes.LongType,true),
                DataTypes.createStructField("click_count",DataTypes.LongType.true)));
            HiveContext  sqlContext=new HiveContext(rdd.context());
           DataFrame dailyAdClickCountByProvinceDF=sqlContext.createDataFrame(rowsRDD,schema);
           dailyAdClickCountByProvinceDF.registerTempTable("tmp_daily_ad_click_count_by_prov");
            DataFrame provinceTop3AdDF=sqlContext.sql("select  date,province,ad_id,click_count from (" +
                    "select date,province,ad_id,click_count," +
                    "  r "+
                    "  from  tmp_daily_ad_click_count_by_prov" +
                    " ) t  where rank>=3");
            return      provinceTop3AdDF.javaRDD();
     }
     rowsDStream.foreachRDD(new Function<JavaRDD<Row>,Void>(){
         public Void call(JavaRDD<Row> rdd){
             rdd.foreachPartition(new VoidFunction<Iterator<Row>>(){
                  public void call(Iterator<Row> iterator){
                      List<AdProvinceTop3>   adProvinceTop3s=new ArrayList<AdProvinceTop3>();
                      while(iterator.hasNext()){
                          Row row=iterator.next();
                          String date=row.getString(1);
                          String province=row.getString(2);
                          long adid=row.getLong(3);
                          long clickCount=row.getLong(4);
                          AdProvinceTop3 adProvinceTop3=new AdProvinceTop3();
                          adProvinceTop3.setDate(date);
                          adProvinceTop3.setProvince(province);
                          adProvinceTop3.setAdid(adid);
                          adProvinceTop3.setClickCount(clickCount);
                           adProvinceTop3s.add(adProvinceTop3);
                      }
                      IAdProvinceTop3DAO  adProvinceTop3DAO=DAOFactory.getAdProvinceTop3DAO();
                      adProvinceTop3DAO.updateBatch(adProvinceTop3s);
                  }
             });
        }
    });
  private  static void calculateAdClickCountByWindow(JavaPairInputDStream<String,String> adRealTimeLogDStream){
         JavaPairDStream<String,Long> pairDStream=adRealTimeLogDStream.mapToPair(new PairFunction<Tuple2<String,String>,String,Long>(){
             public Tuple2<String,Long> call(Tuple2<String,String> tuple){
                       String[] logSplited=tuple._2.split(" ");
                       String  timeMinute=DateUtils.formatTimeMinute(new Date(Long.valueOf(logSplited[0])));
                       long adid=Long.valueOf(logSplited[4]);
                       return  new Tuple2<String,Long>(timeMinute+"_"+adid,1L);
             }
         });
         JavaPairDStream<String,Long> aggrRDD=pairDStream.reductByKeyAndWindow(new Function2<Long,Long,Long>(){
             public  Long call(Long v1,Long v2){
                 return v1+v2;
             }
         },Durations.minutes(60),Durations.seconds(10));
          aggrRDD.foreachRDD(new Function<JavaPairRDD<String,Long>,Void>(){
              public Void call(JavaPairRDD<String,Long> rdd){
                  rdd.foreachPartition(new VoidFunction<Iterator<Tuple2<String,Long>>>(){
                       public   void call(Iterator<Tuple2<String,Long>> iterator){
                                    List<AdClickTrend>  adClickTrends=new ArrayList<AdClickTrend>();
                                    while(iterator.hasNext()){
                                        Tuple2<String,Long> tuple=iterator.next();
                                        String[]  keySplited=tuple._1.splite("_");
                                        String dateMinute=keySplited[0];
                                        long adid=Long.valueOf(keySplited[1]);
                                        long clickCount=tuple._2;
                                        String  date=DateUtils.formatDate(DateUtils.parseDateKey(dateMinute.substring(0,8)));
                                        String hour=dateMinute.substring(8,10);
                                        String minute=dateMinute.substring(10);
                                        AdClickTrend adClickTrend=new AdClickTrend();
                                        adClickTrend.setDate(date);
                                        adClickTrend.setHour(hour);
                                        adClickTrend.setMinute(minute);
                                        adClickTrend.setAdid(adid);
                                        adClickTrend.setClickCount(clickCount);
                                        adClickTrends.add(adClickTrend);
                                    }
                                    IAdClickTrendDAO adClickTrendDAO=DAOFactory.getAdClickTrendDAO();
                                     adClickTrendDAO.updateBatch(adClickTrends);
                       }
                  });
                  return null;
              }
          });





  }

}
