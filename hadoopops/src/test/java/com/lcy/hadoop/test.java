package com.lcy.hadoop;

import org.junit.Test;

import java.util.HashMap;

/**
 * Created by： luochengyue
 * date: 2019/7/9.
 * desc：
 */
public class test {

    @Test
    public void testBo(){
        boolean[] bs = new boolean[3];
        for(int i =0 ;i<bs.length;i++){
            System.out.println(bs[i]);
        }
    }


}
class Solution {
    public int[] twoSum(int[] nums, int target) {
        HashMap<Integer,Integer> remeber = new HashMap();
        for(int i =0;i<nums.length;i++){
            if(remeber.get(target-nums[i])!=null){
                return new int[]{remeber.get(target-nums[i]),i};
            }
            remeber.put(nums[i],i);
        }
        return null;
    }
}
