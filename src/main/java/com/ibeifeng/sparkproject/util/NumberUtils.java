package com.ibeifeng.sparkproject.util;

import java.math.BigDecimal;

/**
 * Created by Administrator on 2017/3/26.
 */
public class NumberUtils {
    public static double   formatDouble(double num,int scale){
            BigDecimal  bd= new  BigDecimal(num);
            return bd.setScale(scale, BigDecimal.ROUND_HALF_UP).doubleValue();
    }


}
