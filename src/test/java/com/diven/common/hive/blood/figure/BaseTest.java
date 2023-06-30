package com.diven.common.hive.blood.figure;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import junit.framework.TestCase;

public class BaseTest extends TestCase{
	
	static String hqls [] = {
			"--***********************************************************************************\n" +
					"--功能描述：优+小区激励金\n" +
					"--创建人:   张广良\n" +
					"--创建日期： 20220708\n" +
					"--修改日期   修改人 修改内容\n" +
					"--\n" +
					"--***********************************************************************************\n" +
					"set hive.support.sql11.reserved.keywords = false;\n" +
					"SET hive.exec.dynamic.partition=true;\n" +
					"SET hive.exec.dynamic.partition.mode=nonstrick;\n" +
					"SET hive.exec.parallel=TRUE;\n" +
					"SET hive.merge.mapfiles=true;\n" +
					"SET hive.merge.mapredfiles=true;\n" +
					"set hive.input.format=org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;\n" +
					"set spark.sql.legacy.timeParserPolicy=LEGACY;\n" +
					"\n" +
					"with area2_mbr as (\n" +
					"select\n" +
					"    count(if(t1.status in(1,3) and substr(t1.ds,1,7)= substr('${ds}',1,7),gold_card_order_sn,null)) pay_amt, --开卡人数\n" +
					"    count(if(t1.status = 3 and substr(refund_time,1,7)= substr('${ds}',1,7),gold_card_order_sn,null)) return_amt, --开卡人数\n" +
					"    count(if(t1.status in(1,3) and substr(t1.ds,1,7)= substr('${ds}',1,7),gold_card_order_sn,null))-count(if(t1.status = 3 and substr(refund_time,1,7)= substr('${ds}',1,7),gold_card_order_sn,null)) real_amt\n" +
					"    ,t2.area_class2_name,\n" +
					"    t2.class2_area_manage_name,\n" +
					"    employee_no,\n" +
					"    substr('${ds}',1,7) ds\n" +
					"from miniso_dwd.dwd_cn_minapp_gold_card_order t1\n" +
					"LEFT JOIN\n" +
					"(\n" +
					"    select\n" +
					"        t1.area_class2_name,\n" +
					"        t1.class2_area_manage_name,\n" +
					"        t1.sap_store_code,\n" +
					"        t2.employee_no\n" +
					"    from miniso_dim.dim_store t1\n" +
					"    left join\n" +
					"    (\n" +
					"        select\n" +
					"            a.nachn employee_name,\n" +
					"            a.pernr employee_no,\n" +
					"            b.kunnr sap_store_code,\n" +
					"            row_number() over(partition by kunnr order by pernr ) rn\n" +
					"        from ods.sap_hr_pa0002 a\n" +
					"        inner join ods.ods_sap_SD_KNA1  b on b.zzctct = a.pernr and b.ds='${ds}'\n" +
					"        where a.ds = '${ds}'\n" +
					"    )t2\n" +
					"    on t1.sap_store_code = t2.sap_store_code\n" +
					"    and t2.rn =1\n" +
					"    where t1.ds = '${ds}'\n" +
					"\n" +
					")t2\n" +
					"on t1.sap_store_code = t2.sap_store_code\n" +
					"where nvl(t1.sap_store_code,'')!= ''\n" +
					"and status in (1,3)\n" +
					"and t2.area_class2_name != '线上门店项目'\n" +
					"and pay_package_name like '%年卡%' --限定年卡\n" +
					"group by\n" +
					"    t2.area_class2_name,\n" +
					"    t2.class2_area_manage_name,\n" +
					"    employee_no,\n" +
					"    substr('${ds}',1,7)\n" +
					")\n" +
					"insert overwrite table miniso_ads.ads_cn_minapp_gold_card_order_bounty_area partition (ds)\n" +
					"select\n" +
					"    pay_amt\n" +
					"    ,return_amt,\n" +
					"    real_amt,\n" +
					"    area_class2_name,\n" +
					"    class2_area_manage_name,\n" +
					"    employee_no,\n" +
					"    case when (real_amt / dayofmonth('${ds}'))>= 140 then 0.8*real_amt\n" +
					"         when (real_amt / dayofmonth('${ds}'))>= 90 and (real_amt / dayofmonth('${ds}')) < 140  then 0.5*real_amt\n" +
					"         else 0.3*real_amt end d,\n" +
					"    'a2',\n" +
					"    date_format(current_timestamp,'yyyy-MM-dd HH:mm:ss'),\n" +
					"    ds\n" +
					"from area2_mbr distribute by ds\n"
	};
	

	/**
	 * 输出标准的json字符串
	 * @param obj
	 */
	public static void printJsonString(Object obj) {
		String str = JSON.toJSONString(obj, 
				SerializerFeature.WriteMapNullValue, 
				SerializerFeature.WriteNullListAsEmpty, 
				SerializerFeature.DisableCircularReferenceDetect, 
				SerializerFeature.PrettyFormat);
		System.out.println(str);
	}
	
}
