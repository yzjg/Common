package com.ibeifeng.sparkproject.spark.page;

import com.ibeifeng.sparkproject.conf.ConfigurationManager;
import com.ibeifeng.sparkproject.constant.Constants;
import com.ibeifeng.sparkproject.dao.IPageSplitConvertRateDAO;
import com.ibeifeng.sparkproject.dao.impl.DAOFactory;
import com.ibeifeng.sparkproject.domain.PageSplitConvertRate;
import com.ibeifeng.sparkproject.util.NumberUtils;
import com.ibeifeng.sparkproject.util.ParamUtils;
import com.ibeifeng.sparkproject.util.SparkUtils;
import jdk.nashorn.internal.runtime.linker.JavaAdapterFactory;


import java.util.HashMap;
import java.util.Map;

/**
 * Created by Administrator on 2017/3/31.
 */
public class PageOneStepConvertRateSpark {
    public  static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName(Constants.SPARK_APP_NAME_PAGE);
        SparkUtils.setMaster(conf);
        JavaSparkContext  sc=new   JavaSparkContext(conf);
        SQLContext  sqlContext=SparkUtils.getSQLContext(sc.sc());
        SparkUtils.mockData(sc,sqlContext);
       long  taskid= ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PAGE);
        ITaskDAO   taskDAO=DAOFactory.getTaskDAO();
        Task  task=taskDAO.findById(taskid);
        if(task==null){
            return ;
        }
        JSONObject      taskParam=JSONObject.parseObject(task.getTaskParam());
        JavaRDD<Row> actionRDD=SparkUtils.getActionRDDByDateRange(sqlContext,taskParam);
        JavaPairRDD<String,Row>  sessionid2actionRDD= getSessionid2actionRDD(actionRDD);
        JavaPairRDD<String,Iterable<Row>>  sessionid2actionRDD=   sessionid2actionRDD.groupByKey();
       JavaPairRDD<String,Integer>  pageSplitRDD=generateAndMatchPageSplit(sc,sessionid2actionRDD,taskParam);
       Map<String,Object>  pageSplitPvMap=pageSplitRDD.countByKey();
        long startPagePv=getStartPagePv(taskParam,sessionid2actionRDD);
        Map<String,Double> convertRateMap=computePageSplitConvertRate(taskParam,pageSplitPvMap,startPagePv);
         persistConvertRate(taskid,convertRateMap);

        SparkUtils.setMaster(conf);
    }


    private   static long getStartPagePv(JSONObject  taskParam,JavaPairRDD<String,Iterable<Row> sessionid2actionsRDD>){
        String  targetPageFlow=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW);
        final long  startPageId=Long.valueOf(targetPageFlow.split(",")[0]);
       JavaRDD<Long>  startPageRDD=sessionid2actionsRDD.flatMap(new FlatMapFunction<Tuple2<String,Iterable<Row>>,Long>(){
           public Iterable<Long>  call(Tuple2<String,Iterable<Row>  tuple>){
               List<Long>  list=new ArrayList<Long>();
               Iterable<Row>  iterator= tuple._2.iterator();
                while(iterator.hasNext()){
                        Row row=iterator.next();
                        long pageid=row.getLong(3);
                        if(pageid==startPageId){
                            list.add(pageid);
                        }
                }
                 return list;
           }
       });
          return  startPageRDD.count();
    }
   private  static Map<String,Double>  computePageSplitConvertRate(JSONObject  taskParam,Map<String,Object> pageSplitPvMap,long  startPagePv){
        Map<String,Double> convertRateMap=new HashMap<String,Double>();
        String[]  targetPages=ParamUtils.getParam(taskParam,Constants.PARAM_TARGET_PAGE_FLOW).split(",");
        long  lastPageSplitPv=0L;
        for(int i=0;i<targetPages.length;i++){
            String targetPageSplit=targetPages[i-1]+"_"+targetPages[i];
            long  targetPageSplitPv=Long.valueOf(String.valueOf(pageSplitPvMap.get(targetPageSplit)));
            double convertRate=0.0;
            if(i==1){
                convertRate= NumberUtils.formatDouble((double)targetPageSplitPv/(double)startPagePv,2);

            }
            else{
                convertRate=NumberUtils.formatDouble((double)targetPageSplitPv/(double)lastPageSplitPv,2);
            }
            convertRateMap.put(targetPageSplit,convertRate);
            lastPageSplitPv=targetPageSplitPv;
        }
            return   convertRateMap;
   }

   private static void persistConvertRate(long taskid,Map<String,Double> convertRateMap){
          StringBuffer buffer=new StringBuffer("");
          for(Map.Entry<String,Double> convertRateEntry:convertRateMap.entrySet()){
              String pageSplit=convertRateEntry.getEntry();
               double   convertRate=convertRateEntry.getValue();
               buffer.append(pageSplit+"="+convertRate+"|");
             }
            String   convertRate=buffer.toString();
            convertRate=convertRate.substring(0,convertRate.length()-1);
       PageSplitConvertRate pageSplitConvertRate=new PageSplitConvertRate();
       pageSplitConvertRate.setTaskid(taskid);
       pageSplitConvertRate.setConvertRate(convertRate);
       IPageSplitConvertRateDAO pageSplitConvertRateDAO= DAOFactory.getPageSplitConvertRateDAO();
       pageSplitConvertRateDAO.insert(pageSplitConvertRate);
   }
   // string bigint   MYSQL中的source命令，执行sql文件的导出语句，
    //   insert into task(      )     select      from   task where task_id=3;
    //整个项目打包，debug configuration->maven  build ->clean package
    //spark-submit   --class   --master    --num-executors    --executor-cores   --driver-memory    --executor-memory  jar  ${1}   位置参数
    //spark-submit    --files   hive-site.xml    --driver-class-path   mysql   spark连接hive的元数据信息的要点是要将hive-site.xml拷贝到spark中的配置的
     //修改权限命令     hadoop  fs  -chmod   777  /tmp/hive-root   hive中的元数据的存储位置
    //  grant all privileges  on spark_project.*   to   'hive'@'%'  identified by 'hive'
    //   flush    privileges;
     //小写的null在hive表中不会解析成NULL的意思



}
