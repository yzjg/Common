package com.ibeifeng.sparkproject;

/**
 * Created by Administrator on 2017/3/27.
 */
@SuppressWarnings("unused")
public class JdbcCRUD {
    public static void main(String[] args){

    }

    private static void insert(){
        Connnection  conn=null;
        Statement  stmt=null;
        try{
           Class.forName("com.mysql.jdbc.Driver");
           conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project","root","root");
           stmt=conn.createStatement();
            String  sql="insert into test_user(name,age) values('张三',26)";
            int  rtn=stmt.executeUpdate(sql);

        }catch(Exception  e){
            e.printStackTrace();
        }finally{
            try {
                if (stmt != null) {
                    stmt.close();
                }
                if(conn!=null){
                    conn.close();
                }
            }catch(Exception  e2){e2.printStackTrace();}
        }
    }
   private      static  void update(){
        Connection  conn=null;
        Statement stmt=null;
        try{
            Class.forName("com.mysql.jdbc.Driver");
           conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project","root","root");
            stmt=conn.createStatement();
            String  sql="update  test_user set  aget=27   where  name='李四'";
            stmt.executeUpdate(sql);
        }
        catch(Exception  e){e.printStackTrace();}
        finally{
            try{
                if(stmt!=null){stmt.close();}
                if(conn!=null){conn.close();}
            }
            catch(Exception  e2){
                e2.printStackTrace();
            }
        }

   }
    private static void delete(){
       Connection conn=null;
       Statement  stmt=null;
       try{
           Class.forName("com.mysql.jdbc.Driver");
           conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project","root","root");
           stmt=conn.creatStatement();
           String sql="delete from  test_user  where  name='库'";
              int  rtn=stmt.executeUpdate(sql);
       }catch(Exception  e){e.printStackTrace();}
       finally{
           try{
               if(stmt!=null){stmt.close();}
               if(conn!=null){conn.close();}
           }catch(Exception   e2){e2.printStackTrace();}
       }
    }
   private static void select(){
        Connection   conn=null;
        Statement  stmt=null;
        ResultSet  rs=null;
        try{
            Class.forName("com.mysql.jdbc.Driver");
            conn=DriverManager.getConnetion("jdbc:mysql://localhost:3306/spark_project","root","root");
            stmt=conn.createStatement();
            String sql="select * from  test_user";
            rs=stmt.executeQuery(sql);
             while(rs.next()){
                  int  id=rs.getInt(1);
                  String name=rs.getString(2);
                  int age=rs.getInt(3);
             }
        }catch(Exception  e1){e1.printStackTrace();}
        finally{
            try{
                if(stmt!=null){stmt.close();}
                if(conn!=null){conn.close();}
            }catch(Exception  e2){
                e2.printStackTrace();
            }
        }
   }
    private  static   void  preparedStatement(){
       Connection  conn=null;
       PreparedStatement pstmt=null;
       try{
           Class.forName("com.mysql.jdbc.Driver");
           conn=DriverManager.getConnection("jdbc:mysql://localhost:3306/spark_project?characterEncoding=utf-8","root","root");
              String sql="insert into test_user(name,age) values(?,?)";
              pstmt=conn.prepareStatement(sql);
              pstmt.setString(1,"你");
              pstmt.setInt(2,26);
             int rtn=pstmt.executeUpdate();
       }
       catch(Exception  e){
           e.printStackTrace();
       }
       finally{
           try{
               if(pstmt!=null){pstmt.close();}
               if(conn!=null){conn.close();}
           }
           catch(Exception  e2){
               e2.printStackTrace();
           }
       }

    }

}
