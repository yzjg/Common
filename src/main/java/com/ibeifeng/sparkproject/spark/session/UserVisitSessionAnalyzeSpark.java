import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;Ypackage com.ibeifeng.sparkproject.spark.session;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.ISessionAggrStatDAO;
import com.ibeifeng.sparkproject.dao.ISessionDetailDAO;
import com.ibeifeng.sparkproject.dao.ISessionRandomExtractDAO;
import com.ibeifeng.sparkproject.dao.ITop10CategoryDAO;
import com.ibeifeng.sparkproject.dao.impl.DAOFactory;
import com.ibeifeng.sparkproject.domain.SessionAggrStat;
import com.ibeifeng.sparkproject.domain.SessionDetail;
import com.ibeifeng.sparkproject.domain.SessionRandomExtract;
import com.ibeifeng.sparkproject.domain.Top10Category;
import com.ibeifeng.sparkproject.mockdData.MockData;
import com.ibeifeng.sparkproject.spark.session.CategorySortKey;
import com.ibeifeng.sparkproject.spark.session.SessionAggrStatAccumulator;
import com.ibeifeng.sparkproject.util.*;
import com.sun.rowset.internal.Row;

import java.util.*;

/**
 * Created by Administrator on 2017/3/27.
 */
public class UserVisitSessionAnalyzeSpark {
          public static void  main(String[] args){
              //task表中的字段有task_id,task_name,create_time,start_time,finish_time,task_type,task_status,task_param
              args=new String[]{"1"};
               SparkConf    conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_SESSION)
                       .set("spark.serializer","org.apache.spark.serializer.KryoSerializer").
                               set("spark.storage.memoryFraction","0.5")
                               .set("spark.default.parallelism","100")
                                .set("spark.shuffle.consolidateFiles","true")
                                .set("spark.shuffle.file.buffer","64")
                                .set("spark.reducer.maxSizeInFlight","24")
                                 .set("spark.shuffle.io.maxRetries","60")
                                 .set("spark.shuffle.io.retryWait","60")



                               .setMaster("local").registerKryoClasses(new Class[]{CategorySortKey.class,IntList.class});
             SparkUtils.setMaster(conf);




               JavaSparkContext sc=new JavaSparkContext(conf);
               SQLContext   sqlContext=getSQLContext(sc.sc());
               SparkUtils.mockData(sc,sqlContext);
               ITaskDAO  taskDAO=DAOFactory.getTaskDAO();
              long taskid = ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_SESSION);
               Task task=taskDAO.findById(taskid);
               if(task==null){
                   return;
               }


               JSONObject   taskParam=JSONObject.parseObject(task.getTaskParam());
              JavaRDD<Row>  actionRDD=getActionRDDByDateRange(sqlContext,taskParam);
               /**持久化       对RDD调用persist的方法，并传入一个持久化的级别
                * persist(StorageLevel.MEMORY_ONLY())=======cache（） 纯内存   无序列化的
                * persist(StorageLevel.MEMORY_ONLY_SER()),
                * persist(StorageLevel.MEMORY_AND_DISK())
                *         StorageLevel.MEMORY_AND_DISK_SER()
                * StorageLevel.DISK_ONLY
                * StorageLevel.MEMORY_ONLY_2()双副本
                *
                *
                * spark.serializer =org.apache.spark.serializer.KryoSeriaaalizer  进行Kryo的序列化的操作
                *spark.locality.wait.(process node rack）     数据本地化的级别
                *process_local node_local  no_pref   rack_local  any
                *conf.set("spark.locality.wait","10")     数据本地化等待时长
                *spark.storage.memoryFraction  0.6       cache操作的内存占比
                * */



             JavaPairRDD<String,Row> sessionid2actionRDD= getSessionid2ActionRDD(actionRDD);
              sessionid2actionRDD=sessionid2actionRDD.persist(StorageLevel.MEMORY_ONLY());

              JavaPairRDD<String,String> sessionid2AggrInfoRDD=aggregateBySession(sqlContext,actionRDD);
              System.out.println(sessionid2AggrInfoRDD.count());
              for(Tuple2<String,String> tuple:sessionid2AggrInfoRDD.take(10)){
                  System.out.println(tuple._2);
              }

              Accumulator<String> sessionAccrStatAccumulator=sc.accumulator("",new SessionAggrStatAccumulator());

              JavaPairRDD<String,String>  filteredSessionid2AggrInfoRDD=filterSessionAndAggrStat(sessionid2AggrInfoRDD,taskParam,sessionAccrStatAccumulator);
              System.out.println(filteredSessionid2AggrInfoRDD.count());
              for(Tuple2<String,String>  tuple:filteredSessionid2AggrInfoRDD){
                  System.out.println(tuple._2);
              }
              filteredSessionid2AggrInfoRDD.count();

              JavaPairRDD<String,Row>  sessionid2detailRDD=getSessionid2detailRDD(filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
                  sessionid2detailRDD=sessionid2detailRDD.persist(StorageLevel.MEMORY_ONLY());
              randomExtractSession(sc,task.getTaskid(), filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
              calculateAndPersistAggrStat(sessionAccrStatAccumulator.value(),task.getTaskid());


                getTop10Category(taskid,filteredSessionid2AggrInfoRDD,sessionid2actionRDD);
                List<Tuple2<CategorySortKey,String>>  top10CategoryList=getTop10Category(task.getTaskid(),sessionid2detailRDD);
                 getTop10Session(sc,task.getTaskid(),top10CategoryList,sessionid2detailRDD);




               sc.close();
          }

          private  static void  getTop10Session( JavaSparkContext sc,final long taskid,
                 List<Tuple2<CategorySortKey,String>>  top10CategoryList, JavaPairRDD<String,Row> sessionid2detailRDD){
                    List<Tuple2<Long,Long>>  top10CategoryIdList=new ArrayList<Tuple2<Long,Long>>();
                   for(Tuple2<CategorySortKey,String>  category:top10CategoryList){
                       long categoryid=Long.valueOf(StringUtils.getFieldFromConcatString(category._2,"\\|",Constants.FIELD_CATEGORY_ID));
                       top10CategoryIdList.add(new Tuple2<Long,Long>(categoryid,categoryid));
                   }
                   JavaPairRDD<Long,Long>  top10CategoryIdRDD=sc.parallelizePairs(top10CategoryIdList);
                                  JavaPairRDD<String,Iterable<Row>>   sessionid2detailsRDD=sessionid2detailRDD.groupByKey();
                  JavaPairRDD<Long,String>  categoryid2sessionCountRDD=sessionid2detailsRDD.flatMapToPair(
                          newPairFlatMapFunction<Tuple2<String,Iterable<Row>>,Long,String>(){
                             private  static final long serialVersionUID=1L;
                             public     Iterable<Tuple2<Long,String>>  call(Tuple2<String,Iterable<Row>> tuple) throws Exception{
                                       String sessionid=tuple._1;
                                       Iterator<Row>  iterator=tuple._2.iterator();
                                       Map<Long,Long>  categoryCountMap=new HashMap<Long,Long>();
                                       while(iterator.hasNext()){
                                           Row row=iterator.next();
                                           if(row.get(6)!=null){
                                               long  categoryid=row.getLong(6);
                                             Long count=categoryCountMap.get(categoryid);
                                             if(count==null){count=0L;}
                                             count++;
                                             categoryCountMap.put(categoryid,count);
                                           }
                                       }
                             List<Tuple2<Long,String>>  list=new ArrayList<Tuple2<Long,String>>();
                              for(Map.Entry<Long,Long> categoryCountEntry:categoryCountMap.entrySet()){
                                  long categoryid=categoryCountEntry.getKey();
                                  long count=categoryCountEntry.getValue();
                                   String  value=sessionid+","+count;
                                   list.add(new Tuple2<Long,String>(categoryid,value));
                              }
                              return  list;
                  }
              });
              JavaPairRDD<Long,String>   top10CategorySessionCountRDD=top10CategoryIdRDD.join(categoryid2sessionCountRDD).mapToPair(
                          new PairFunction<Tuple2<Long,Tuple2<Long,String>>,Long,String>(){
                              private static final long  serialVersionUID=1L;
                              public Tuple2<Long,String> call(Tuple2<Long,Tuple2<Long,String>>  tuple) throws Exception{
                                  return  new Tuple2<Long,String>(tuple._1,tuple._2._2);
                              }
                          });
                 JavaPairRDD<Long,Iterable<String>>  top10CategorySessionCountRDD=top10CategorySessionCountRDD.groupByKey();
                 JavaPairRDD<String,String>  top10SessionRDD=top10CategorySessionCountRDD.flatMapToPair(new  PariFlatMapFunction<Tuple2<Long,Iterable<String>>,String,String>(){
                     private static final long serialVersinUID=1L;
                     public  Iterable<Tuple2<String,String>>  call(Tuple2<Long,Iterable<String>> tuple) throws Exception{
                         long categoryid=tuple._1;
                         Iterable<String> iterator=tuple._2.iterator();
                         String[] top10Sessions=new String[10];
                         while(iterator.hasNext()){
                             String sessionCount=iterator.next();
                             String  sessionid=sessionCount.split(",")[0];
                             long count=Long.valueOf(sessionCount.split(",")[1]);
                             for(int i=0;i<top10Sessions.length;i++){
                                 if(top10Sessions[i]==null){
                                     top10Sessions[i]=sessionCount;
                                     break;
                                 }
                                 else{
                                     long _count=Long.valueOf(top10Sessions[i].split(",")[1]);
                                     if(count>_count){
                                            for(int j=9;j>i;j--){
                                                top10Sessions[j]=top10Sessions[j-1];
                                            }
                                            top10Sessions[i]=sessionCount;
                                            break;
                                     }
                                 }
                             }

                         }
                          List<Tuple2<String,String>>   list=new ArrayList<Tuple2<String,String>>();
                         for(String  sessionCount:top10Sessions){
                                 String sessionid=sessionCount.split(",")[0];
                                 long count=Long.valueOf(sessionCount.split(",")[1]);
                                 Top10Session top10Session=new Top10Session();
                                 top10Session.setTaskid(taskid);
                                 top10Session.setCategoryid(categoryid);
                                 top10Session.setSessionid(sessionid);
                                 top10Session.setClickCount(clickCount);
                                 ITop10SessionDAO  top10SessionDAO=DAOFactory.getTop10SessionDAO();
                                top10SessionDAO.insert(top10Session);
                                list.add(new Tuple2<String,String>(sessionid,sessionid));
                         }
                         return  list;
                     }
                 });
                 JavaPairRDD<String,Tuple2<String,Row>>  sessionDetailRDD=top10SessionRDD.join(sessionid2detailRDD);
                 sessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>(){
                     private static final long serialVersionUID=1L;
                     public void call(Tuple2<String,Tuple2<String,Row>>   tuple)throws Exception{
                                   Row row=tuple._2._2;
                                    SessionDetail sessionDetail=new SessionDetail();
                                    sessionDetail.setTaskid(taskid);
                                    sessionDetail.setUserid(row.getLong(1));
                                    sessionDetail.setSessionid(row.getString(2));
                                    sessionDetail.setPageid(row.getLong(3));
                                    sessionDetail.setActionTime(row.getString(4));
                                    sessionDetail.setSearchKeyword(row.getString(5));
                                     sessionDetail.setClickCategoryId(row.getLong(6));
                                    sessionDetail.setClickProductId(row.getLong(7));
                                    sessionDetail.setOrderCategoryIds(row.getString(8));
                                    sessionDetail.setOrderProductIds(row.getString(9));
                                    sessionDetail.setPayCategoryIds(row.getString(10));
                                    sessionDetail.setPayProductIds(row.getString(11));
                                    ISessionDetailDAO  sessionDetailDAO=DAOFactory.getSessionDetailDAO();
                                    sessionDetailDAO.insert(sessionDetail);



                     }


                 });
     /**spark-submit  --class   --num-executors  --driver-memory    --executor-memory  --executor-cores   --master yarn-cluster   -queue  root.default  --conf
      *--conf    spark.yarn.executor.memoryOverhead=2048    基于yarn的提交模式，堆外溢出的处理情况
      *--conf    spark.core.connection.ack.wait.timeout=300   调节连接的超时时长300秒，默认的情况下是1分钟的时长，
      *spark.default.parallelism并行度，设置task的处理的数量  textFile(,partition数量)
      * DAGScheduler  TaskScheudler  会重复的发送spark的请求处理的，
      *set("spark.shuffle.consolidateFiles","true") map端输出文件合并的设置
      *spark.shuffle.file.buffer,默认32k，map端内存缓冲
      * spark.shuffle.memoryFraction,0.2    reduce端的内存占比
      *spark.shuffle.manager:sort  hash     tungsten-sort
      * spark.shuffle.sort.bypassMergeThreshold:200
      *spark.reducer.maxSizeInFlight=48M    reducer端的内存缓冲
      *spark.shuffle.io.maxRetries=3
      * spark.shuffle.io.retryWait=5s
      * --conf  spark.driver.extraJavaOptions="-XX:PermSize=128M   -XX:MaxPermSize=256M"
      *设置永久代的大小 避免内存溢出
      *
      *val lines=sc.textFile("hdfs://")
      * val words=lines.flatMap(_.split(" "))
      * val pairs=words.map((_,1))
      *val wordcount=pairs.reduceByKey(_+_)
      * wordcount.collect()
      *ExecutorService   threadPool=Executors.newFixedThreadPool(1);
      * threadPool.submit(new Runnable(){
      *     public void run(){}
      * });
      *    return  new Tuple2<String,Row>("-999",RowFactory. createRow("-9999"));
      *sparkContext.checkpointFile("hdfs://")
      * RDD.checkpoint()
      *  shuffle算子的并行度的，传递第二个参数  countByKey没有并行度的设置
      *
      *
      *
      *
      * */



          }









          private static JavaPairRDD<String,Row> getSessionid2detailRDD(JavaPairRDD<String,String> sessionid2aggrInfoRDD,JavaPairRDD<String,Row> sessionid2actionRDD){
              JavaPairRDD<String,Row>  sessionid2detailRDD=sessionid2aggrInfoRDD.join(sessionid2actionRDD).mapToPair(
                      new PairFunction<Tuple2<String,Tuple2<String,Row>>,String,Row>(){
                          private static final  long serialVersionUID=1L;
                          public Tuple2<String,Row>  call(Tuple2<String,Tuple2<String,Row>> tuple) throws Exception{
                              return  new Tuple2<String,Row>(tuple._1,tuple._2._2);
                          }

                      });
                return  sessionid2detailRDD;
          }








          private  static List<Tuple2<CategorySortKey,String>> getTop10Category(long  taskid, JavaPairRDD<String,String> filteredSessionid2AggrInfoRDD, JavaPairRDD<String,Row>  sessionid2actionRDD){
             JavaPairRDD<String,Row>   sessionid2detailRDD=filteredSessionid2AggrInfoRDD.join(sessionid2actionRDD).mapToPair(new PairFunction<Tuple2<String,Row>,String,Row>(){
                      public Tuple2<String,Row>  call(Tuple2<String,Tuple2<String,Row>> tuple) throws  Exception{
                                          return new Tuple2<String,Row>(tuple._1,tuple._2._2);
                      }
                });
               JavaPairRDD<Long,Long> categoryidRDD=sessionid2detailRDD.flatMapToPair(
                       new PairFlatMapFunction<Tuple2<String,Row>,Long,Long>(){
                           private static  final long serialVersionUID=1L;
                           public Iterable<Tuple2<Long,Long>>  call(Tuple2<String,Row> tuple) throws Exception{
                               Row row=tuple._2;
                               List<Tuple2<Long,Long>>  list=new ArrayList<Long,Long>();
                                Long clickCategoryId=row.getLong(6);
                                if(clickCategoryId!=null){
                                    list.add(new Tuple2<Long,Long>(clickCategoryId,clickCategoryId));
                                }
                                 String orderCategoryIds=row.getString(8);
                                if(orderCategoryIds!=null){
                                    String[]  orderCategoryIdsSplited=orderCategoryIds.split(",");
                                    for(String  orderCategoryId:orderCategoryIdsSplited){
                                        list.add(new Tuple2<Long,Long>(Long.valueOf(orderCategoryId),Long.valueOf(orderCategoryId)));
                                    }
                                }
                              String payCategoryIds=row.getString(10);
                               if(payCategoryIds!=null){
                                   String[] payCategoryIdsSplited=payCategoryIds.split(",");
                                    for(String payCategoryId:payCategoryIdsSplited){
                                        list.add(new Tuple2<Long,Long>(Long.valueOf(payCategoryId),Long.valueOf(payCategoryId)));
                                    }
                               }
                                return list;
                           }
                          });

                    categoryidRDD=categoryidRDD.distinct();

                        JavaPairRDD<String,Row> clickActionRDD=
                                sessionid2detailRDD.filter(new Function<Tuple2<String,Row>,Boolean>(){
                                         private static final  long serivalVersionUID=1L;
                                         public Boolean call(Tuple2<String,Row> tuple) throws Exception{
                                              Row row=tuple._2;
                                       //  return Long.valueOf(row.getLong(6))!=null?true:false;
                                         return row.get(6)!=null?true:false;
                                  }
                                });
                       JavaPairRDD<Long,Long>  clickCategoryIdRDD=clickActionRDD.mapToPair(new PairFunction<Tuple2<String,Row>,Long,Long>(){
                           private static final long serialVersionUID=1L;
                           public Tuple2<Long,Long>   call(Tuple2<String,Row> tuple) throws Exception{
                                long clickCategoryId=tuple._2.getLong(6);
                                 return new Tuple2<Long,Long>(clickCategoryId,1L);
                           }
                       });

                       JavaPairRDD<String,Long> mappedClickCategoryIdRDD= clickCategoryIdRDD.mapToPair(
                               new PairFunction<Tuple2<Long,Long>,String,Long>(){
                                   private static final long serialVersionUID=1L;
                                   public Tuple2<String,Long> call(Tuple2<Long,Long> tuple)throws Exception{
                                       Random random=new Random();
                                      int prefix=random.nextInt(10);
                                        return  new Tuple2<String,Long>(prefix+"_"+tuple._1,tuple._2);
                                   }
                               });
                      JavaPairRDD<String,Long> firstAggrRDD= mappedClickCategoryIdRDD.reduceByKey(new Function2<Long,Long,Long>(){
                               private  static final long serialVersionUID=1L;
                                 public Long call(Long v1,Long v2) throws Exception{
                                   return v1+v2;
                                  }
                                     });
                       JavaPairRDD<Long,Long> restoredRDD=firstAggrRDD.mapToPair(new PairFunction<Tuple2<String,Long>,Long,Long>(){
                             private static final long  serialVersionUID=1L;
                             public Tuple2<Long,Long> call(Tuple2<String,Long> tuple) throws Exception{
                                 long categoryId=Long.valueOf(tuple._1.split("_")[1]);
                                 return new Tuple2<Long,Long>(categoryId,tuple._2);
                             }

                       });
                       JavaPairRDD<Long,Long>  globalAggrRDD=restoredRDD.reduceByKey(
                               new Function2<Long,Long,Long>(){
                                   private static final long serialVersionUID=1L;
                                   public Long   call(Long v1,Long  v2) throws  Exception{
                                       return v1+v2;
                                   }});

                List<Tuple2<Long,Row>>  userInfos=userid2InfoRDD.collect();
                final BroadCast<List<Tuple2<Long,Row>>>   userInfosBroadcast=sc.broadcast(userInfos);
               JavaPairRDD<String,String>  tunedRDD=userInfosBroadcast.mapToPair(
                       new PairFunction<Tuple2<Long,String>,String,String>(){
                           private static final long serialVersionUID=1L;
                            public Tuple2<String,String>  call(Tuple2<Long,String> tuple) throws Exception{
                                  List<Tuple2<Long,Row>> userInfos=userInfoBroadcast.value();
                                  Map<Long,Row> userInfoMap=new HashMap<Long,Row>();
                                  for(Tuple2<Long,Row>  userInfo:userInfos){
                                      userInfoMap.put(userInfo._1,userInfo._2);
                                  }
                            }
                       });
                    JavaPairRDD<Long,String> sampleRDD=userid2PartAggrInfoRDD.sample(false,0.1,9);
                    JavaPairRDD<Long,Long> mappedSampleRDD=sampleRDD.mapToPair(new PairFunction<Tuple2<Long,String>,Long,Long>(){
                        private static final long serialVersionUID=1L;
                        public Tuple2<Long,Long> call(Tuple2<Long,String>  tuple) throws Exception{
                            return  new Tuple2<Long,Long>(tuple._1,1L);
                         }
                    });
                              JavaPairRDD<Long,Long>  computedSampledRDD=   mappedSampleRDD.reduceByKey(new Function2<Long,Long,Long>(){
                                     private  static final long serialVersionUID=1L;
                                     public Long call(Long v1,Long v2) throws Exception{
                                         return v1+v2;
                                     }
                                 });
                              JavaPairRDD<Long,Long> reversedSampleRDD=computedSampledRDD.mapToPair(new PairFunction<Tuple2<Long,Long>,Long,Long>(){
                                   private static final long verialVersionUID=1L;
                                   public Tuple2<Long,Long>  call(Tuple2<Long,Long> tuple) throws Exception{
                                       return new  Tuple2<Long,Long>(tuple._2,tuple._1);
                                   }
                               });
                         final  Long skewedUserid=reversedSampleRDD.sortByKey(false).take(1).get(0)._2;
                                          userid2PartAggrInfoRDD.filter(new Function<Tuple2<Long,String>,Boolean>(){
                                              private  static final long serialVersionUID=1L;
                                              public Boolean call(Tuple2<Long,String> tuple) throws Exception{
                                                  return  tuple._1.equals(skewedUserid);
                                              }
                                          });




              JavaPairRDD<Long,Long>  clickCategoryId2CountRDD=clickCategoryIdRDD.reduceByKey(
                               new Function2<Long,Long,Long>(){
                                   public Long call(Long  v1,Long  v2){
                                       return  v1+v2;
                                   }
                               });
                  JavaPairRDD<String,Row> orderActionRDD=sessionid2detailRDD.filter(
                          new Function<Tuple2<String,Row> ,Boolean>(){
                              private static final  long  serialVersionUID=1L;
                              public Boolean call(Tuple2<String,Row> tuple) throws Exception {
                                  Row row = tuple._2;
                                  return row.getString(8) != null ? true : false;
                              }
                          });
                    JavaPairRDD<Long,Long> orderCategoryIdRDD=clickActionRDD.flatMapToPair(
                            new PairFlatMapFunction<Tuple2<String,Row>,Long,Long>() {
                                private static final long serialVersionUID = 1L;

                                public Iterable<Tuple2<Long, Long>> call(Tuple2<String, Row> tuple) throws Exception {
                                    Row row = tuple._2;
                                    String orderCategoryIds = row.getString(8);
                                    String[] orderCategoryIdsSplited = orderCategoryIds.split(",");
                                    List<Tuple2<Long, Long>> list = new ArrayList<Tuple2<Long, Long>>();
                                    for (String orderCategoryId : orderCategoryIdsSplited) {
                                        list.add(new Tuple2<Long, Long>(Long.valueOf(orderCategoryId), 1L));
                                    }
                                  return list;
                                }
                            });
                            JavaPairRDD<Long,Long> orderCategoryId2CountRDD=orderCategoryIdRDD.reduceByKey(
                                    new Function<Long,Long,Long>(){
                                        private static final long serialVersionUID=1L;
                                        public  Long  call(Long v1,Long  v2) throws Exception{
                                             return  v1+v2;
                                        }
                                    });
                        JavaPairRDD<String,Row>  payActionRDD=sessionid2detailRDD.filter(
                                new Function<Tuple2<String,Row>,Boolean>(){
                                     public Boolean call(Tuple2<String,Row> tuple) throws Exception{
                                         Row row=tuple._2;
                                         return row.getString(10)!=null?true:false;
                                     }
                                });
                      JavaPairRDD<String,Row>   payCategoryIdRDD=payActionRDD.flatMapToPair(
                              new PairFlatMapFunction<Tuple2<String,Row>,Long,Long>(){
                                  private static final long serialVersionUID=1L;
                                  public Iterable<Tuple2<Long,Long>>  call(Tuple2<String,Row> tuple) throws Exception{
                                      Row row=tuple._2;
                                      String  payCategoryIds=row.getString(10);
                                      String[] payCategoryIdsSplited=payCategoryIds.split(",");
                                      List<Tuple2<Long,Long>>  list=new  ArrayList<Tuple2<Long,Long>>();
                                      for(String   payCategoryId:payCategoryIdsSplited){
                                          list.add(new Tuple2<Long,Long>(Long.valueOf(payCategoryId),1L));
                                      }
                                      return list;
                                  }
                              });
                      JavaPairRDD<Long,Long> payCategoryId2CountRDD=payCategoryIdRDD.reduceByKey(
                              new Function2<Long,Long,Long>(){
                                  private static final long  serialVersionUID=1L;
                                  public Long call(Long v1,Long v2) throws Exception{
                                      return v1+v2;
                                  }
                              });



              JavaPairRDD<Long,String> categoryid2countRDD=joinCategoryAndData(categoryidRDD,clickCategoryId2CountRDD,orderCategoryId2CountRDD,payCategoryId2CountRDD);
                      JavaPairRDD<CategorySortKey,String>  sortKey2countRDD=categoryid2countRDD.mapToPair(
                              new PairFunction<Tuple2<Long,String>,CategorySortKey,String>(){
                                  private   static final long  serialVersionUID=1L;
                                  public Tuple2<CategorySortKey,String>  call(Tuple2<Long,String> tuple) throws Exception{
                                      String countInfo=tuple._2;
                                      long  clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(
                                              countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
                                      long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(
                                              countInfo,"\\|",Constants.FIELD_ORDER_COUNT
                                      ));
                                      long payCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT));
                                                 CategorySortKey sortKey=new CategorySortKey(clickCount,orderCount,payCount);
                                      return new Tuple2<CategorySortKey,String>(sortKey,countInfo);
                                  }
                              });
                    JavaPairRDD<CategorySortKey,String>  sortedCategoryCountRDD =sortKey2countRDD.sortByKey(false);//降序





                  List<Tuple2<CategorySortKey,String>>   top10CategoryList=sortedCategoryCountRDD.take(10);
                   for(Tuple2<CategorySortKey,String> tuple:top10CategoryList){
                          String countInfo=tuple._2;
                          long categoryid=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ CATEGORY_ID));
                          long clickCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_CLICK_COUNT));
                          long orderCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_ORDER_COUNT));
                          long  payCount=Long.valueOf(StringUtils.getFieldFromConcatString(countInfo,"\\|",Constants.FIELD_PAY_COUNT));
                          Top10Category category=new Top10Category();
                          category.setTaskid(taskid);
                          category.setCategoryid(categoryid);
                          category.setClickCount(clickCount);
                          category.setOrderCount(orderCount);
                          category.setPayCount(payCount);
                          ITop10CategoryDAO top10CategoryDAO=DAOFactory.getTop10CategoryDAO();
                          top10CategoryDAO.insert(category);
                   }
              return   top10CategoryList;
          }



          public static  JavaPairRDD<String,Row>  getSessionid2ActionRDD(JavaRDD<Row>   actionRDD){
//              return  actionRDD.mapToPair(new PairFunction<Row,String,Row>{
//                  public Tuple2<String,Row>  call(Row  row) throws Exception{
//                      return   new Tuple2<String,Row>(row.getString(2),row);
//                  }
//              });

             return   actionRDD.mapPartitionsToPair(new PairFlatMapFunction<Iterator<Row>,String,Row>(){
                  private static final long serialVersionUID=1L;
                  public Iterable<Tuple2<String,Row>>  call(Iterator<Row>  iterator) throws  Exception{
                               List<Tuple2<String,Row>>  list=new ArrayList<Tuple2<String,Row>>();
                                while(iterator.hasNext()){
                                    Row row=iterator.next();
                                    list.add(new  Tuple2<String,Row>(row.getString(3),row));
                                }
                                return list;
                               }
                                 });


          }






         private static void randomExtractSession(JavaSparkContext sc,final long taskid,JavaPairRDD<String,String> sessionid2AggrInfoRDD,
                                                  JavaPairRDD<String,Row> sessionid2actionRDD){
                JavaPairRDD<String,String> time2sessionidRDD=sessionid2AggrInfoRDD.mapToPair(
                        new  PairFunction<Tuple2<String,String>,String,String>(){
                            private  static  final long serialVersionUID=1L;
                            public Tuple2<String,String> call(Tuple2<String,String> tuple) throws Exception{
                                      String sessionid=tuple._1;
                                      String aggrInfo=tuple._2;
                                      String startTime=StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_START_TIME);
                                     String  dateHour=DateUtils.getDateHour(startTime);
                                    return new Tuple2<String,String>(dateHour,aggrInfo);
                            }
                        });
                Map<String,Object> countMap=time2sessinidRDD.countByKey();
                Map<String,Map<String,Long>> dateHourCountMap=new HashMap<String,Map<String,Long>>();
                for(Map.Entry<String,Object> countEntry:countMap.entrySet()){
                    String  dateHour=countEntry.getKey();
                    long   count=Long.valueOf(String.valueOf(countEntry.getValue()));
                    String  date=dateHour.split("_")[0];
                    String  hour=dateHour.split("_")[1];
                    Map<String,Long>  hourCountMap=dateHourCountMap.get(date);
                     if(hourCountMap==null){
                         hourCountMap=new HashMap<String,Long>();
                         dateHourCountMap.put(date,hourCountMap);
                     }
                        hourCountMap.put(hour,count);
                }
             int  extractNumberPerDay=100/dateHourCountMap.size();
          final  Map<String,Map<String,List<Integer>>>   dateHourExtractMap=new HashMap<String,Map<String,List<Integer>>>();
             Random  random=new Random();
             for(Map.Entry<String,Map<String,Long>>  dateHourCountEntry:dateHourCountMap.entrySet()){
                  String date=dateHourCountEntry.getKey();
                  Map<String,Long>  hourCountMap=dateHourCountEntry.getValue();
                  long  sessionCount=0L;
                  for(long hourCount:hourCountMap.values()){
                      sessionCount+=hourCount;
                  }
                  Map<String,List<Integer>>  hourExtractMap=dateHourExtractMap.get(date);
                 if(hourExtractMap==null){
                     hourExtractMap=new HashMap<String,List<Integer>>();
                     dateHourExtractMap.put(date,hourExtractMap);
                 }

                  for(Map.Entry<String,Long> hourCountEntry:hourCountMap.entrySet()){
                      String hour=hourCountEntry.getKey();
                      long  count=hourCountEntry.getValue();
                      int  hourExtractNumber=(int)((double)count/(double)sessionCount*extractNumberPerDay);

                      if(hourExtractNumber>count){
                          hourExtractNumber=(int)count;
                      }


                      List<Integer>  extractIndexList=hourExtractMap.get(hour);
                      if(extractIndexList==null){
                          extractIndexList=new ArrayList<Integer>();
                          hourExtractMap.put(hour,extractIndexList);
                      }
                        for(int i=0;i<hourExtractNumber;i++){
                          int  extractIndex=random.nextInt((int)count);
                          while(extractIndexList.contains(extractIndex)){
                              extractIndex=random.nextInt((int)count);
                          }
                          extractIndexList.add(extractIndex);
                        }
                  }
             }



             Map<String,Map<String,IntList>>  fastutilDateHourExtractMap=new HashMap<String,Map<String,IntList>>();
             for(Map.Entry<String,Map<String,List<Integer>>>    dateHourExtractEntry:dateHourExtractMap.entrySet()){
                 String date=dateHourExtractEntry.getKey();
                 Map<String,List<Integer>>  hourExtractMap=dateHourExtractEntry.getValue();
                 Map<String,IntList>    fastutilHourExtractMap=new HashMap<String,IntList>();
                  for(Map.Entry<String,List<Integer>>  hourExtractEntry:hourExtractMap.entrySet()){
                      String  hour=hourExtractEntry.getKey();
                      List<Integer>  extractList=hourExtractEntry.getValue();
                      IntList   fastutilExtractList=new IntArrayList();
                      for(int i=0;i<extractList.size();i++){
                           fastutilExtractList.add(extractList.get(i));
                      }
                         fastutilHourExtractMap.put(hour,fastutilExtractList);
                  }
                  fastutilDateHourExtractMap.put(date,fastutilHourExtractMap);



             }






          final    Broadcast<Map<String,Map<String,IntList>>>   dateHourExtractMapBroadcast=sc.broadcast(fastutilDateHourExtractMap);
             JavaPairRDD<String,Iterable<String>>  time2sessionsRDD=time2sessionidRDD.groupByKey();
             JavaPairRDD<String,String>  extractSessionidsRDD=time2sessionidsRDD.flatMapToPair(
                     new PairFlatMapFunction<Tuple2<String,Iterable<String>>,String,String>(){
                         private static final  long  serialVersionUID=1L;
                         public Iterable<Tuple2<String,String>>  call(Tuple2<String,Iterable<String>>  tuple) throws Exception{
                                     List<Tuple2<String,String>>   extractSessionids=new ArrayList<Tuple2<String,String>>();
                                      String dateHour=tuple._1;
                                      String date=dateHour.split("_")[0];
                                      String  hour=dateHour.split("_")[1];
                                      Iterator<String>  iterator=tuple._2.iterator();

                                      Map<String,Map<String,IntList>>  dateHourExtractMap=dateHourExtractMapBroadcast.getValue();
                                      List<Integer>  extractIndexList=dateHourExtractMap.get(date).get(hour);
                                      ISessionRandomExtractDAO  sessionRandomExtractDAO=DAOFactory.getSessionRandomExtractDAO();
                                      int index=0;
                                      while(iterator.hasNext()){
                                          String sessionAggrInfo=iterator.next();
                                          if(extractIndexList.contains(index)){
                                              String sessionid=StringUtils.getFieldFromConcatString(
                                                      sessionAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                                              SessionRandomExtract  sessionRandomExtract=new SessionRandomExtract();
                                              sessionRandomExtract.setTaskid(taskid);
                                              sessionRandomExtract.setSessionid(sessionid);
                                              sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",
                                                      Constants.FIELD_START_TIME));

                                              sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS);
                                              sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS));
                                              sessionRandomExtractDAO.insert(sessionRandomExtract);
                                              extractSessionids.add(new Tuple2<String,String>(sessionid,sessionid));
                                          }
                                          index++;
                                      }

                                    return extractSessionids;
                         }
                     }

             );




            JavaRDD<String>  extractSessionidsRDD=time2sessionsRDD.flatMap(
                    new FlatMapFunction<Tuple2<String,Iterable<String>>,String>(){
                        private  static final long  serialVersionUID=1L;
                        public Iterable<String>  call(Tuple2<String,Iterable<String>>  tuple) throws Exception{
                                  List<String>   extractSessionids=new ArrayList<String>();
                                  String dateHour=tuple._1;
                                  String  date=dateHour.split("_")[0];
                                  String  hour=dateHour.split("_")[1];
                                  Iterator<String> iterator=tuple._2.iterator();
                                  List<Integer>  extractIndexList=dateHourExtractMap.get(date).get(hour);
                                  ISessionRandomExtractDAO sessionRandomExtractDAO=DAOFactory.getSessionRandomExtractDAO();
                                  int  index=0;
                                  while(iterator.hasNext()){
                                      String sessionAggrInfo=iterator.next();
                                        if(extractIndexList.contains(index)){
                                            String sessionid=StringUtils.getFieldFromConcatString(
                                                    sessionAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                                            SessionRandomExtract  sessionRandomExtract=new SessionRandomExtract();
                                              sessionRandomExtract.setTaskid(taskid);
                                              sessionRandomExtract.setSessionid(sessionid);
                                               sessionRandomExtract.setStartTime(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",
                                                       Constants.FIELD_START_TIME));

                                               sessionRandomExtract.setSearchKeywords(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_SEARCH_KEYWORDS);
                                               sessionRandomExtract.setClickCategoryIds(StringUtils.getFieldFromConcatString(sessionAggrInfo,"\\|",Constants.FIELD_CLICK_CATEGORY_IDS));
                                               sessionRandomExtractDAO.insert(sessionRandomExtract);
                                               extractSessionids.add(sessionid);
                                        }
                                      index++;
                                  }
                                  return   extractSessionids;
                        }
                    }
            );
             JavaPairRDD<String,Tuple2<String,Row>>    extractSessionDetailRDD=extractSessionidsRDD.join(sessionid2actionRDD);
             extractSessionDetailRDD.foreach(new VoidFunction<Tuple2<String,Tuple2<String,Row>>>(){
                 private static final long serialVersionUID=1L;
                 public  void call(Tuple2<String,Tuple2<String,Row>> tuple) throws Exception{
                          Row row=tuple._2._2;
                          SessionDetail sessionDetail=new SessionDetail();
                           sessionDetail.setTaskid(taskid);
                           sessionDetail.setUserid(row.getLong(1));
                           sessionDetail.setSessionid(row.getString(2));
                           sessionDetail.setPageid(row.getLong(3));
                           sessionDetail.setActionTime(row.getString(4));
                           sessionDetail.setSearchKeyword(row.getString(5));
                           sessionDetail.setClickCategoryId(row.getLong(6));
                           sessionDetail.setClickProductId(row.getLong(7));
                           sessionDetail.setOrderCategoryIds(row.getString(8));
                           sessionDetail.setOrderProductIds(row.getString(9));
                           sessionDetail.setPayCategoryIds(row.getString(10));
                           sessionDetail.setPayProductIds(row.getString(11));
                           ISessionDetailDAO sessionDetailDAO=DAOFactory.getSessionDetailDAO();
                           sessionDetailDAO.insert(sessionDetail);





                 }
             });
         }



































          private  static void calculateAndPersistAggrStat(String value,long taskid){
              long  session_count=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|", Constants.SESSION_COUNT));
              long visit_length_1s_3s=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1s_3s));
              long visit_length_4s_6s=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_4s_6s));
              long  visit_length_7s_9s=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_7s_9s));
              long  visit_length_10s_30s=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10s_30s));
              long  visit_length_30s_60s=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30s_60s));
              long  visit_length_1m_3m=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_1m_3m));
              long  visit_length_3m_10m=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_3m_10m));
              long  visit_length_10m_30m=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_10m_30m));
              long  visit_length_30m=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.TIME_PERIOD_30m));
              long step_length_1_3=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_1_3));
              long step_length_4_6=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_4_6));
              long step_length_7_9=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_7_9));
              long step_length_10_30=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_10_30));
              long step_length_30_60=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_30_60));
              long step_length_60=Long.valueOf(StringUtils.getFieldFromConcatString(value,"\\|",Constants.STEP_PERIOD_60));
              double visit_length_1s_3s_ratio= NumberUtils.formatDouble((double)visit_length_1s_3s/(double)session_count,2);
              double  visit_length_4s_6s_ratio=NumberUtils.formatDouble((double)visit_length_4s_6s/(double)session_count,2);
              double visit_length_7s_9s_ratio=NumberUtils.formatDouble((double)visit_length_7s_9s/(double)session_count,2);
              double visit_length_10s_30s_ratio=NumberUtils.formatDouble((double)visit_length_10s_30s/(double)session_count,2);
              double visit_length_30s_60s_ratio=NumberUtils.formatDouble((double)visit_length_30s_60s/(double)session_count,2);
              double visit_length_1m_3m_ratio=NumberUtils.formatDouble((double)visit_length_1m_3m/(double)session_count,2);
              double visit_length_3m_10m_ratio=NumberUtils.formatDouble((double)visit_length_3m_10m/(double)session_count,2);
              double visit_length_10m_30m_ratio=NumberUtils.formatDouble((double)visit_length_10m_30m/(double)session_count,2);
              double visit_length_30m_ratio=NumberUtils.formatDouble((double)visit_length_30m/(double)session_count,2);
              double step_length_1_3_ratio=NumberUtils.formatDouble((double)step_length_1_3/(double)session_count,2);
              double step_length_4_6_ratio=NumberUtils.formatDouble((double)step_length_4_6/(double)session_count,2);
              double step_length_7_9_ratio=NumberUtils.formatDouble((double)step_length_7_9/(double)session_count,2);
              double step_length_10_30_ratio=NumberUtils.formatDouble((double)step_length_10_30/(double)session_count,2);
              double step_length_30_60_ratio=NumberUtils.formatDouble((double)step_length_30_60/(double)session_count,2);
              double step_length_60_ratio=NumberUtils.formatDouble((double)step_length_60/(double)session_count,2);
              SessionAggrStat sessionAggrStat=new SessionAggrStat();
              sessionAggrStat.setTaskid(taskid);
              sessionAggrStat.setSession_count(session_count);
              sessionAggrStat.setVisit_length_1s_3s_ratio(visit_length_1s_3s_ratio);
              sessionAggrStat.setVisit_length_4s_6s_ratio(visit_length_4s_6s_ratio);
              sessionAggrStat.setVisit_length_7s_9s_ratio(visit_length_7s_9s_ratio);
              sessionAggrStat.setVisit_length_10s_30s_ratio(visit_length_10s_30s_ratio);
              sessionAggrStat.setVisit_length_30s_60s_ratio(visit_length_30s_60s_ratio);
              sessionAggrStat.setVisit_length_1m_3m_ratio(visit_length_1m_3m_ratio);
              sessionAggrStat.setVisit_length_3m_10m_ratio(visit_length_3m_10m_ratio);
              sessionAggrStat.setVisit_length_10m_30m_ratio(visit_length_10m_30m_ratio);
              sessionAggrStat.setVisit_length_30m_ratio(visit_length_30m_ratio);
              sessionAggrStat.setStep_length_1_3_ratio(step_length_1_3_ratio);
              sessionAggrStat.setStep_length_4_6_ratio(step_length_4_6_ratio);
              sessionAggrStat.setStep_length_7_9_ratio(step_length_7_9_ratio);
              sessionAggrStat.setStep_length_10_30_ratio(step_length_10_30_ratio);
              sessionAggrStat.setStep_length_30_60_ratio(step_length_30_60_ratio);
              sessionAggrStat.setStep_length_60_ratio(step_length_60_ratio);
              ISessionAggrStatDAO sessionAggrStatDAO= DAOFactory.getSessionAggrStatDAO();
              sessionAggrStatDAO.insert(sessionAggrStat);

          }



























          private static JavaPairRDD<String,String>  filterSessionAndAggrStat(JavaPairRDD<String,String> sessionid2AggrInfoRDD
          ,final JSONObject  taskParam,final Accumulator<String>  sessionAggrStatAccumulator){
              String startAge=ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
              String  endAge=ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
              String  professionals=ParamUtils.getParam(taskParam,Constants.PARAM_PROFESSIONALS);
              String  cities=ParamUtils.getParam(taskParam,Constants.PARAM_CITIES);
              String sex=ParamUtils.getParam(taskParam,Constants.PARAM_SEX);
              String  keywords=ParamUtils.getParam(taskParam,Constants.PARAM_SEARCH_KEYWORDS);
              String   categoryIds=ParamUtils.getParam(taskParam,Constants.PARAM_CATEGORY_IDS);

               String  _parameter=(startAge!=null?Constants.PARAM_START_AGE+"="+ startAge+"|":"")
                       +(endAge!=null? Constants.PARAM_END_AGE+"="+endAge+"|":"")
                        +(professionals!=null?Constants.PARAM_PROFESSIONALS+"="+professionals+"|":"")
                         +(cities!=null?Constants.PARAM_CITIES+"="+cities+"|":"")
                       +(sex!=null?Constants.PARAM_SEX+"="+sex+"|":"")
                       +(keywords!=null?Constants.PARAM_SEARCH_KEYWORDS+"="+keywords+"|":"")
                       +(categoryIds!=null?Constants.PARAM_CATEGORY_IDS+"="+categoryIds:"");
                    if(_parameter.endsWith("\\|")){
                        _parameter=_parameter.substring(0,_parameter.length()-1);
                    }
                   final String parameter=_parameter;
              JavaPairRDD<String,String>  filteredSessionid2AggrInfoRDD=sessionid2AggrInfoRDD.filter(
                      new Function<Tuple2<String,String>,Boolean>(){
                          private static final long  serialVersionUID=1L;
                          public Boolean call(Tuple2<String,String>  tuple) throws Exception{
                              String  aggrInfo=tuple._2;
                              int age=Integer.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_AGE));
                               String   startAge=ParamUtils.getParam(taskParam,Constants.PARAM_START_AGE);
                               String endAge=ParamUtils.getParam(taskParam,Constants.PARAM_END_AGE);
                              if(!ValidUtils.between(aggrInfo,Constants.FIELD_AGE,parameter,Constants.PARAM_START_AGE,Constants.PARAM_END_AGE)){
                                  return false;
                              }
                              if(!ValidUtils.in(aggrInfo,Constants.FIELD_PROFESSIONAL,parameter,Constants.PARAM_PROFESSIONALS)){
                                  return  false;
                              }
                              if(!ValidUtils.in(aggrInfo,Constants.FIELD_CITY,parameter,Constants.PARAM_CITIES)){
                                  return false;
                              }
                              if(!ValidUtils.equal(aggrInfo,Constants.FIELD_SEX,parameter,Constants.PARAM_SEX)){
                                  return false;
                              }
                              if(!ValidUtils.in(aggrInfo,Constants.FIELD_SEARCH_KEYWORDS,parameter,Constants.PARAM_SEARCH_KEYWORDS)){
                                  return  false;
                              }
                              if(!ValidUtils.in(aggrInfo,Constants.FIELD_CLICK_CATEGORY_IDS,parameter,Constants.PARAM_CATEGORY_IDS)){
                                  return false;
                              }
                               sessionAggrStatAccumulator.add(Constants.SESSION_COUNT);
                                long   visitLength=Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_VISIT_LENGTH));
                                long stepLength=Long.valueOf(StringUtils.getFieldFromConcatString(aggrInfo,"\\|",Constants.FIELD_STEP_LENGTH));
                                      calculateStepLength(stepLength);
                                      calculateVisitLength(visitLength);

                              return true;
                          }
                        private void calculateVisitLength(long  visitLength){
                              if(visitLength>=1 && visitLength<=3){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1s_3s);
                              }
                              else if(visitLength>=4 && visitLength<=6){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_4s_6s);
                              }
                              else if(visitLength>=7  && visitLength <=9){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_7s_9s);
                              }
                              else if(visitLength>=10 && visitLength <=30){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10s_30s);
                              }
                              else if(visitLength>30 &&visitLength <=60){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30s_60s);
                              }
                              else  if(visitLength >60 && visitLength <=180){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_1m_3m);
                              }
                              else if(visitLength>180 && visitLength<=600){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_3m_10m);
                              }
                              else if(visitLength >600 && visitLength<=1800){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_10m_30m);
                              }
                              else if(visitLength >1800){
                                  sessionAggrStatAccumulator.add(Constants.TIME_PERIOD_30m);
                              }
                        }
                        private     void calculateStepLength(long stepLength){
                            if(stepLength>=1 && stepLength <=3){
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_1_3);
                            }
                            else if(stepLength >=4 && stepLength<=6){
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_4_6);
                            }
                            else if(stepLength >=7 && stepLength <=9){
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_7_9);
                            }
                            else if(stepLength >=10 && stepLength <=30){
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_10_30);
                            }
                            else if(stepLength >30 && stepLength <=60){
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_30_60);
                            }
                            else if(stepLength >60){
                                sessionAggrStatAccumulator.add(Constants.STEP_PERIOD_60);
                            }
                        }
                      });
                      return   filteredSessionid2AggrInfoRDD;
          }






          private static SQLContext  getSQLContext(SparkContext sc){
                  boolean  local= ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
                  if(local){
                      return  new SQLContext(sc);
                  }
                  return  HiveContext(sc);
          }
       private static   void mockData(JavaSparkContext sc,SQLContext  sqlContext){
              boolean local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
              if(local){
                  MockData.mock(sc,sqlContext);
              }
       }




       private  static JavaRDD<Row>  getActionRDDByDateRange(SQLContext sqlContext,JSONObject taskParam){
           String startDate=ParamUtils.getParam(taskParam,Constants.PARAM_START_DATE);
           String  endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
           String sql="select  * from  user_visit_action  where   date >='"+startDate+"'"
                   +" and   date<='"+endDate+"'";
           DataFrame  actionDF=sqlContext.sql(sql);
           //actionDF.javaRDD().repartition(1000);
           return  actionDF.javaRDD();
       }
    private static  JavaPairRDD<String,String> aggregateBySession(SQLContext sqlContext,JavaRDD<Row> actionRDD){
           JavaPairRDD<String,Row> session2ActionRDD=actionRDD.mapToPair(
                   new PairFunction<Row,String,Row>(){
                       private static final long  serialVersionUID=1L;
                       public Tuple2<String,Row>   call(Row row) throws Exception{
                           return new Tuple2<String,Row>(row.getString(2),row);
                       }
                   }
           );
           JavaPairRDD<String,Iterable<Row>>  sessionid2ActionsRDD=session2ActionRDD.groupByKey();
           JavaPairRDD<Long,String>  userid2PartAggrInfoRDD=sessionid2ActionsRDD.mapToPair(
                   new PairFunction<Tuple2<String,Iterable<Row>>,Long,String>(){
                       private static final long serivalVersionUID=1L;
                       public Tuple2<Long,String>   call(Tuple2<String,Iterable<Row>>   tuple) throws Exception{
                            String  sessionid=tuple._1;
                            Iterator<Row> iterator=tuple._2.iterator();
                            StringBuffer  searchKeywordsBuffer=new StringBuffer("");
                            StringBuffer  clickCategoryIdsBuffer=new StringBuffer("");
                            Long userid=null;
                            Date  startTime=null;
                            Date endTime=null;
                            int stepLength=0;
                            while(iterator.hasNext()){
                                Row row=iterator.next();
                                if(userid==null){userid=row.getLong(1);}
                                String searchKeyword=row.getString(5);
                                Long  clickCategoryId=row.getLong(6);
                                if(StringUtils.isNotEmpty(searchKeyword)){
                                    if(!searchKeywordBuffer.toString().contains(searchKeyword)){
                                        searchKeywordsBuffer.append(searchKeyword+",");
                                    }
                                }
                                if(clickCategoryId!=null){
                                    if(!clickCategoryIdsBuffer.toString().contains(String.valueOf(clickCategoryId))){
                                        clickCategoryIdsBuffer.append(clickCategoryId);
                                    }
                                }
                                     Date  actionTime= DateUtils.parseTime(row.getString(4));
                                    if(startTime==null){
                                        startTime=actionTime;
                                    }
                                    if(endTime==null){
                                        endTime=actionTime;
                                    }
                                    if(actionTime.before(startTime)){
                                        startTime=actionTime;
                                    }
                                    if(actionTime.after(endTime)){
                                        endTime=actionTime;
                                    }
                                stepLength++;
                            }
                            //    +(StringUtils.isNotEmpty(searchKeywords)?Constants.FIELD_SEARCH_KEYWORDS + "=" + searchKeywords+"|":"")
                            String searchKeywords=StringUtils.trimComma(searchKeywordsBuffer.toString());
                            String clickCategoryIds= StringUtils.trimComma(clickCategoryIdsBuffer.toString());
                           long visitLength=(endTime.getTime()-startTime.getTime())/1000;
                           String  partAggInfo=Constants.FIELD_SESSION_ID+"="+sessionid+"|"
                                    +Constants.FIELD_SEARCH_KEYWORDS+"="+searchKeywords+"|"
                                    + Constants.FIELD_CLICK_CATEGORY_IDS+"="+clickCategoryIds
                                    +Constants.FIELD_VISIT_LENGTH+"="+visitLength+"|"
                                    +Constants.FIELD_STEP_LENGTH+"="+stepLength+"|"
                                      +Constants.FIELD_START_TIME+"="+DateUtils.formatTime(startTime);
                            return new Tuple2<Long,String>(userid,partAggInfo);
                       }
                   });
              String sql="select  * from    user_info";
              JavaRDD<Row> userInfoRDD=sqlContext.sql(sql).javaRDD();
              JavaPairRDD<Long,Row>  userid2InfoRDD=userInfoRDD.mapToPair(
                      new PariFunction(Row,Long,Row){
                          private   static final   long serialVersionUID=1L;
                           public Tuple2<Long,Row>  call(Row  row) throws Exception{
                                  return new Tuple2<Long,Row>(row.getLong(0),row);
                           }
                      });
               JavaPairRDD<Long,Tuple2<String,Row>>   userid2FullInfoRDD=
                       userid2PartAggrInfoRDD.join(userid2InfoRDD);
               JavaPairRDD<String,String>  sessionid2FullAggrInfoRDD=userid2FullInfoRDD.mapToPair(
                       new PairFunction<Tuple2<Long,Tuple2<String,Row>>,String,String>(){
                           private static final long serialVersionUID=1L;
                           public Tuple2<String,String>  call(Tuple2<Long,Tuple2<String,Row>>   tuple) throws Exception{
                                   String partAggInfo=tuple._2._1;
                                   Row  userInfoRow=tuple._2._2;
                                   String sessionid=StringUtils.getFieldFromConcatString(partAggrInfo,"\\|",Constants.FIELD_SESSION_ID);
                                   int age=userInfoRow.getInt(3);
                                   String professional=userInfoRow.getString(4);
                                   String city=userInfoRow.getString(5);
                                   String sex= userInfoRow.getString(6);
                                   String fullAggrInfo=partAggInfo+"|"
                                           + Constants.FIELD_AGE + "="+age+"|"
                                           + Constants.FIELD_PROFESSIONAL+"="+professional+"|"
                                           + Constants.FIELD_CITY+"="+city+"|"
                                           + Constants.FIELD_SEX+"="+sex;
                                  return  new Tuple2<String,String>(sessionid,fullAggrInfo);
                           }
                       }
               );
           return sessionid2FullAggrInfoRDD;
    }



}
