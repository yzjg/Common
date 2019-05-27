package com.ibeifeng.sparkproject;

/**
 * Created by Administrator on 2017/3/27.
 */
public class TaskDAOTest {
    public static void main(String[] args){
       ITaskDAO taskDAO=DAOFactory.getTaskDAO();
       Task task=taskDAO.findById(2);
       System.out.println(task.getTaskName());
    }
}
