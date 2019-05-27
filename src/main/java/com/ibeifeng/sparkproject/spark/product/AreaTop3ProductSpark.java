import com.ibeifeng.sparkproject.spark.product.ConcatLongStringUDF;
import com.ibeifeng.sparkproject.spark.product.GroupConcatDistinctUDA;
import com.ibeifeng.sparkproject.spark.product.RandomPrefixUDF;ypackage com.ibeifeng.sparkproject.spark.product;

import com.ibeifeng.sparkproject.spark.product.RemoveRandomPrefixUDF;
import com.sun.rowset.internal.Row;
import sun.security.pkcs11.wrapper.Constants;

/**
 * Created by Administrator on 2017/4/1.
 */
public class AreaTop3ProductSpark {
    public static void main(String[] args){
        SparkConf conf=new SparkConf().setAppName("AreaTop3ProductSpark");
        SparkUtils.setMaster(conf);
        JavaSparkContext  sc=new JavaSparkContext(conf);
        SQLContext  sqlContext=SparkUtils.getSQLContext(sc.sc());
        //sqlContext.setConf("spark.sql.shuffle.partitions",1000); 默认的并行度是200  groupByKey(1000)
         sqlContext.setConf("spark.sql.autoBroadcastJoinThreshold","20971520");



        sqlContext.udf().register("concat_long_string",new ConcatLongStringUDF(),DataTypes.StringType);
        sqlContext.udf().register("group_concat_distinct",new GroupConcatDistinctUDA());
        sqlContext.udf().register("random_prefix",new RandomPrefixUDF(),DataTypes.StringType);
        sqlContext.udf().register("remove_random_prefix",new RemoveRandomPrefixUDF(),DataTypes.StringType);
        SparkUtils.mockData(sc,sqlContext);
        ITaskDAO   taskDAO=DAOFactory.getTaskDAO();
        long    taskid=ParamUtils.getTaskIdFromArgs(args,Constants.SPARK_LOCAL_TASKID_PRODUCT);
        Task task=taskDAO.findById(taskid);
        JSONObject  taskParam=JSONObject.parseObject(task.getTaskParam());
        String startDate=ParamUtils.getParam(taskParam, Constants.PARAM_START_DATE);
        String endDate=ParamUtils.getParam(taskParam,Constants.PARAM_END_DATE);
        JavaPairRDD<Long,Row>   cityid2clickActionRDD=getcityid2ClickActionRDDByDate(sqlContext,startDate,endDate);
        JavaPairRDD<Long,Row>   cityid2cityInfoRDD=getcityid2CityInfoRDD(sqlContext);
        generateTempClickProductBasicTable(sqlContext,cityid2clickActionRDD,cityid2cityInfoRDD);
        generateTempAreaProductClickCountTable(sqlContext);
        generateTempAreaFullProductClickCountTable(sqlContext);
        JavaRDD<Row>  areaTop3ProductRDD=getAreaTop3ProductRDD(sqlContext);

            sc.close();
    }
    private static  JavaPairRDD<Long,Row>  getClickActionRDDByDate(SQLContext  sqlContext,String    startDate,String    endDate){
        String sql="SELECT  CITY_ID , CLICK_PRODUCT_ID   PRODUCT_ID "
                +"  FROM    user_visit_action "
                +"  WHERE  click_product_id IS NOT NULL"
                +"  AND  click_product_id !='NULL'"
                +" AND  click_product_id  !='null'  "
                +" AND  date>='"+startDate+"' "
                +" AND   date<='"+endDate+"'";
                DataFrame   clickActionDF=sqlContext.sql(sql);
                JavaRDD<Row>   clickActionRDD=clickActionDF.javaRDD();
                JavaPairRDD<Long,Row> cityid2clickActionRDD=clickActionRDD.mapToPair(
                new PairFunction<Row,Long,Row>(){
                    private  static final long serialVersionUID=1L;
                    public Tuple2<Long,Row> call(Row row){
                        Long  cityid=row.getLong(0);
                        return  new Tuple2<Long,Row>(cityid,row);
                    }
                });
        return   cityid2clickActionRDD;
    }
    private static  JavaPairRDD<Long,Row>  getCityInfoRDD(SQLContext  sqlContext){
        String url=null;
        String   user=null;
        String password=null;
        boolean  local=ConfigurationManager.getBoolean(Constants.SPARK_LOCAL);
        if(local){
            url=ConfigurationManager.getProperty(Constants.JDBC_URL);
            user=ConfigurationManager.getProperty(Constants.JDBC_USER);
            password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD);
        }else {
            url=ConfigurationManager.getProperty(Constants.JDBC_URL_PROD);
            user=ConfigurationManager.getProperty(Constants.JDBC_USER_PROD);
            password=ConfigurationManager.getProperty(Constants.JDBC_PASSWORD_PROD);
        }
      Map<String,String>  options=new HashMap<String,String>();
        options.put("url",url);
        options.put("dbtable","city_info");
        options.put("user",user);
        options.put("password",password);
        DataFrame  cityInfoDF=sqlContext.read().format("jdbc").options(options).load();
         JavaRDD<Row>  cityInfoRDD=cityInfoDF.javaRDD();
         JavaPairRDD<Long,Row>   cityid2cityInfoRDD=cityInfoRDD.mapToPair(
                 new PairFunction<Row,Long,Row>(){
                     private  static final long  serialVersionUID=1L;
                     public  Tuple2<Long,Row> call(Row  row){
                         long   cityid=Long.valueOf(String.valueOf(row.get(0)));
                             new  Tuple2<Long,Row>(cityid,row);
                     }
                 });
                  return    cityid2cityInfoRDD;
    }
   private      static  void  generateTempClickProductBasicTable(SQLContext  sqlContext,JavaPairRDD<Long,Row> cityid2clickActionRDD,JavaPairRDD<Long,Row> cityid2cityInfoRDD){
        JavaPairRDD<Long,Tuple2<Row,Row>>  joinedRDD=cityid2clickActionRDD.join(cityid2cityInfoRDD);
        JavaRDD<Row> mappedRDD=joinedRDD.map(
                new Function<Tuple2<Long,Tuple2<Row,Row>>>(){
                    public Row call(Tuple2<Long,Tuple2<Row,Row>> tuple){
                        long cityid=tuple._1;
                        Row clickAction=tuple._2._1;
                        Row  cityInfo=tuple._2._2;
                        long productid=clickAction.getLong(1);
                        String cityName=cityInfo.getString(1);
                        String  area=cityInfo.getString(2);
                        return  RowFactory.create(cityid,cityName,area,productid);
                    }
                });
         List<StructField>  structFields=new ArrayList<StructField>();

        structFields.add(DataTypes.createStructField("city_name",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("area",DataTypes.StringType,true));
        structFields.add(DataTypes.createStructField("product_id"),DataTypes.LongType,true));
        StructType  schema=DataTypes.createStructType(structFields);
        DataFrame     df=sqlContext.createDataFrame(mappedRDD,schema);
        df.registerTempTable("tmp_click_product_basic");
   }
 private  static    void generateTempAreaProductClickCountTable(SQLContext  sqlContext){
       //case   when  then  when then else  end
       String sql="select  area ,  case  when  area='东北' or area ='华东'  then  'A级'" +
               " when  area ='华南' or  area ='华中'  then  'B级' when  area ='西北' or  area ='西南'  then  'C级' ELSE  'D级' END  area_level, product_id, count(*)  click_count,group_concat_distinct(concat_long_string(cityid,cityname,':')  city_infos) "
               +" from  tmp_click_product_basic "
               +" group by  area,product_id";
       DataFrame df=sqlContext.sql(sql);
      df.registerTempTable("tmp_area_product_click_count");
 }
  private  static void  generateTempAreaFullProductClickCountTable(SQLContext sqlContext){
       String sql="select  tapcc.area, tapcc.product_id,tapcc.click_count,tapcc.city_infos,pi.product_name,"
               +" if (get_json_object(pi.extend_info,'product_status')=0,'自营商品','第三方商品')"
               +" from  tmp_area_product_click_count  tapcc"
               +" join  product_info pi  on   tapcc.product_id=pi.product_id ";
          JavaRDD<Row>   rdd=sqlContext.sql("select  * from  product_info");
          JavaRDD<Row>   flattedRDD=rdd.flatMap(new FlatMapFunction<Row,Row>(){
              public Iterable<Row>  call(Row row){
                  List<Row>   list=new ArrayList<Row>();
                  for(int  i=0;i<10;i++){
                      long productid=row.getLong(0);
                      String  _productid=i+"_"+productid;
                      Row   _row=RowFactory.create(_productid,row.get(i),row.get(2));
                      list.add(_row);
                  }
                    return list;
              }
          });
           StructType  schema=DataType.createStructType(Arrays.asList(
                  DataTypes.createStructField("product_id",DataTypes.StringType,true),
                  DataTypes.createStructField("product_name",DataTypes.StringType,true),
                  DataTypes.createStructField("product_status",DataTypes.StringType,true)));
             DataFrame  _df=sqlContext.createDataFrame(flattedRDD,schema);
             _df.registerTempTable("tmp_product_info");
       DataFrame  df=sqlContext.sql(sql);
       df.registerTempTable("tmp_area_fullprod_click_count");
  }
   private static  JavaRDD<Row>  getAreaTop3ProductRDD(SQLContext  sqlContext){
       String sql=" select  area, product_id,click_count,city_infos,product_name,product_status from  (select  area,product_id,click_count,city_infos,product_name,product_status, "
               +" ROW_NUMBER() OVER (PARTITION BY area ORDER BY click_count DESC  )  rank "
               +" from  tmp_area_fullprod_click_count ) t  where t.rank<=3 ";
       String sql1="select  product_id_area,count(click_count) click_count, group_concat_distinct(city_infos) city_infos from  ("
                + "select  remove_random_prefix(product_id_area)  product_id_area,click_count,city_infos from ("
               +"select  "
               +"product_id_area ,"
               +"count(*)  click_count"
               +"group_concat_distinct(concat_long_string(city_id,city_name,':')) city_infos"
               +" from  ("
               +"select  random_prefix(concat_long_string(product_id,area,':'),10)  product_id_area,"
               +"city_id,"
               +"city_name"
               +" from  tmp_click_product_basic ) t1  group by  product_id_area  ) t2  )t3  group  by product_id_area";
         DataFrame df=sqlContext.sql(sql);
         return  df.javaRDD();
   }
}
