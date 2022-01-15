package org.apache.flink;

import org.apache.flink.api.java.utils.ParameterTool;

/**
 * @author zhuyufeng
 * @version 1.0
 * @date 2022-01-14
 * @Description:
 */
public class test {
    public static void main(String[] args) {
        ParameterTool config = ParameterTool.fromArgs(args);
        System.out.println(config.get("input"));
    }
}
