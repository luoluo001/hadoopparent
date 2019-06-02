package com.lcy.joinbean;

import com.lcy.hadoop.mr.join.JoinBean;

/**
 * Created by luo on 2019/6/2.
 */
public class JoinBeanTes {
    public static void main(String[] args) {
        JoinBean joinBean = new JoinBean();
        joinBean.setSId(1);
        joinBean.setCName("aaa");
        joinBean.setCId(2);
        joinBean.setSName("bbb");
        System.out.println(joinBean.toString());
    }
}
