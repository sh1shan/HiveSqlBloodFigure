package com.diven.common.hive.blood.figure;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.serializer.SerializerFeature;
import junit.framework.TestCase;

public class BaseTest extends TestCase{
	
	static String hqls [] = {
			"with tmp0 as (\n" +
					"select\n" +
					"    a.ds,\n" +
					"    a.prod_id as sku_code,\n" +
					"    a.sap_store_code,\n" +
					"    a.stock_prod_qty_1d_ste,\n" +
					"    a.stock_prod_qty_1d_stb,\n" +
					"    sale_qty\n" +
					"from miniso_ads.ads_scm_store_jxcrpt_1d a\n" +
					"left join dim.dim_store_info_lastest t1 on a.sap_store_code=t1.sap_store_code\n" +
					"where a.ds > date_sub('${ds}',28) and  a.ds <= '${ds}' and t1.sts_state = '3A'\n" +
					"union all\n" +
					"select\n" +
					"        a.ds,\n" +
					"        a.sku_code,\n" +
					"        a.sap_store_code ,\n" +
					"        stock_prod_qty_1d_ste ,\n" +
					"        tock_prod_qty_1d_stb ,\n" +
					"        a.sale_qty\n" +
					"    from miniso_dws.dws_rpt_store_goods_sale_query_df a\n" +
					"    left join dim.dim_store_info_lastest t1 on a.sap_store_code=t1.sap_store_code\n" +
					"    where a.ds > date_sub('${ds}',28) and  a.ds <= '${ds}' and t1.sts_state = '3A'\n" +
					")\n" +
					",\n" +
					"tmp as(\n" +
					"    select\n" +
					"           a.sku_code,\n" +
					"           a.sap_store_code,\n" +
					"           count(case when stock_prod_qty_1d_ste > 0 and stock_prod_qty_1d_stb > 0 and ds > date_sub('${ds}',7) then a.sap_store_code end ) as store_day_7,     --近7天符合条件门店数 --日均销门店数\n" +
					"           count(case when stock_prod_qty_1d_ste > 0 and stock_prod_qty_1d_stb > 0 and ds > date_sub('${ds}',28) then a.sap_store_code end) as store_day_28,    --近28天符合条件门店数 --日均销门店数\n" +
					"           sum(case when stock_prod_qty_1d_ste > 0 and stock_prod_qty_1d_stb > 0 and ds > date_sub('${ds}',7) then a.sale_qty else 0 end) as sale_qty_7,  --近七天符合条件销售数量  --日均销销量\n" +
					"           sum(case when stock_prod_qty_1d_ste > 0 and stock_prod_qty_1d_stb > 0 and ds > date_sub('${ds}',28) then a.sale_qty else 0 end) as sale_qty_28  --近28天符合条件销售数量  --日均销销量\n" +
					"    from (\n" +
					"        select\n" +
					"               ds,\n" +
					"               sku_code,\n" +
					"               sap_store_code,\n" +
					"               sum(stock_prod_qty_1d_ste) as stock_prod_qty_1d_ste,\n" +
					"               sum(stock_prod_qty_1d_stb) as stock_prod_qty_1d_stb,\n" +
					"               sum(sale_qty) as sale_qty\n" +
					"        from tmp0\n" +
					"        group by ds,sku_code,sap_store_code\n" +
					"    )a\n" +
					"    group by a.sku_code,a.sap_store_code\n" +
					")\n" +
					",store_area as (\n" +
					"select\n" +
					"       a.sap_store_code,\n" +
					"case when a.area_class1_name = '西藏区' then '西藏区' else b.area end as area\n" +
					"from miniso_dim.dim_store_lastest  a\n" +
					"left join miniso_ods.ods_youdata_442_miniso_sccm_sales_area b on a.comm_opt_manager = b.manager\n" +
					"where a.area_class1_name in ('西藏区','华东经营大区','华北经营大区','华西经营大区','华南经营大区','华中经营大区')\n" +
					")\n" +
					"\n" +
					"insert overwrite table miniso_ads.ads_ds_miniso_sccm_delivery_goods_store_detail partition(ds)\n" +
					"select\n" +
					"    a.stat_date                -- '统计日期'\n" +
					"   ,a.wrh_code                 -- '仓库代码'\n" +
					"   ,a.wrh_name                 -- '仓库名称'\n" +
					"   ,'C1' as cate1_code         --'大类2代码'\n" +
					"   ,t.cate1_name               -- '大类2名称'\n" +
					"   ,t.cate_code                -- '细类代码'\n" +
					"   ,t.cate_name                -- '细类名称'\n" +
					"   ,a.sku_code                 -- '商品代码'\n" +
					"   ,t.sku_name                --'商品名称'\n" +
					"   ,t.pkg_price                -- '组合价'\n" +
					"   ,a.store_code               -- '门店代码'\n" +
					"   ,a.store_name               -- '门店名称'\n" +
					"   ,d.area  --运营区域\n" +
					"\n" +
					"   ,(c.sale_qty_7/c.store_day_7)  as avg_sales_qty_7days      -- '单店日均销量（7天）'\n" +
					"   ,(c.sale_qty_28/c.store_day_28)  as avg_sales_qty_28days     -- '单店日均销量（28天）'\n" +
					"\n" +
					"   ,b.hav_stock_store_qty      -- '当前有库存门店数'\n" +
					"   ,b.hav_sales_rate_7days     -- '7天动销率'\n" +
					"   ,b.being_wrh_turnover_days   -- '即将入仓周转'\n" +
					"   ,b.wrh_turnover_days        -- '仓库周转'\n" +
					"   ,a.zh_days                  -- '门店周转'\n" +
					"\n" +
					"   ,c.sale_qty_7 as sales_qty_7days          -- '销售量-7天（总）'\n" +
					"   ,c.sale_qty_28 as sales_qty_28days         -- '销售量-28天（总）'\n" +
					"   ,date_format(current_timestamp(), 'yyyy-MM-dd HH:mm:ss')\n" +
					"   ,a.ds\n" +
					"from miniso_ads.tmp_05_ads_ds_miniso_sccm_goods_wrh_detail_mid a\n" +
					"left join miniso_ads.ads_ds_miniso_sccm_goods_wrh_detail_mid b on a.wrh_code = b.wrh_code and a.sku_code = b.sku_code and b.ds ='${ds}'\n" +
					"left join tmp c on a.sku_code = c.sku_code and a.store_code = c.sap_store_code\n" +
					"left join store_area d on a.store_code = d.sap_store_code\n" +
					"left join miniso_ads.tmp_01_ads_ds_miniso_sccm_goods_detail_mid t on a.sku_code = t.sku_code and t.ds ='${ds}'\n" +
					"where a.ds ='${ds}'\n"
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
