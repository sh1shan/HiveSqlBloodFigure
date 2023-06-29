package com.diven.common.hive.blood.utils;

import java.util.List;

public class HqlUtil {

    public static String ListToString(List<String> hqls) {
        StringBuffer buffer = new StringBuffer();
        if (hqls != null) {
            for (String sql : hqls) {
                //去除空格
                sql = sql.trim();
                //替换写在末尾的注释内容
                sql = sql.replaceAll("--.*","");
                // 替换多行注释
                sql = sql.replaceAll("/\\*[\\s\\S]*?\\*/","");
                boolean flag = false;
                for (String line : sql.split("\n")) {
                    //和正则的有重复，这是作者的，不影响程序执行
                    if (line.trim().startsWith("--")) {
                        continue;
                    }
                    if (!line.trim().isEmpty()) {
                        flag = true;
                        buffer.append(line).append("\n");
                    }
                }
                if (flag && !sql.endsWith(";")) {
                    buffer.append(";");
                }
            }
        }
        return buffer.toString();
    }

}
