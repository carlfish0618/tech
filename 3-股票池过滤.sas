/*** 各种股票池过滤和统计 ***/
/** pool_table 可以有两种选择:
(0) zl_change(2-专利局数据.sas的输出结果)
(1) gxjs_stock_pool (1-基础表准备.sas的结果)
(2) union_pool(0和1合并的结果)

三个股票池的格式都包括: stock_code, year(year=2014表示2014/6/30日得到的，适用于2014/7/1-2015/6/30的股票)
***/

/** 股票池过滤的筛选条件包括:
(1) 假设前一年12月31日之前应该上市。若认为是6月调仓，即上市时间超过半年
(2) research_pct缺失或者等于0的股票
**/

/** 核心步骤:
(1) 生成每年股票池，剔除上市条件不符合的股票
(2) 增加财报数据
(3) 计算区分度指标: research_pct等
(4) 剔除research_pct不符合条件的股票
(5) 一些数据的统计

**/

/** ！！！！！！pool_table: 外部变量 */
/*%LET pool_table = gxjs_stock_pool;*/
/*%LET pool_table = union_pool;*/
/*%LET pool_table = zl_change;*/

/** Step1: 生成每年股票池 */

/* 剔除未上市的企业 */
/* 假设前一年12月31日之前应该上市。若认为是6月调仓，即上市时间超过半年 */
/** 一共剔除50个样本 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.list_date, B.delist_date, B.bk
	FROM &pool_table. A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.year, A.stock_code;
QUIT;
DATA &pool_table.;
	SET tmp;
	IF list_date >= mdy(12,31,year-1) THEN mark = 1;
	ELSE IF not missing(delist_date) AND delist_date <= mdy(12,31,year-1) THEN mark = 2;
	ELSE mark = 0;
RUN;
DATA &pool_table.(drop = mark list_date delist_date);
	SET &pool_table.;
	IF mark = 0;
	end_date = mdy(6,30,year);
	FORMAT end_date yymmdd10.;
RUN;
/** 调整为6月最后一个交易日 */
%adjust_date_modify(busday_table=busday , raw_table=&pool_table. ,colname=end_date,  output_table=&pool_table., is_forward = 0 );
DATA &pool_table.(drop = adj_end_date end_date_is_busday);
	SET &pool_table.;
	end_date = adj_end_Date;
RUN;
PROC SORT DATA = &pool_table. NODUPKEY;
	BY _ALL_;
RUN;


/* Step2:增加财报数据 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.*
	FROM &pool_table. A LEFT JOIN product.report_data B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
%MACRO add_year_data();
	%DO i = 2012 %TO 2015;
		DATA tmp;
			SET tmp;
			IF year = &i. THEN DO;
				research0 = research%sysevalf(&i.-1);
				research1 = research%sysevalf(&i.-2);
				research2 = research%sysevalf(&i.-3);
				assets0 = assets%sysevalf(&i.-1);
				assets1 = assets%sysevalf(&i.-2);
				assets2 = assets%sysevalf(&i.-3);
				revenue0 = revenue%sysevalf(&i.-1);
				revenue1 = revenue%sysevalf(&i.-2);
				revenue2 = revenue%sysevalf(&i.-3);
			END;
		RUN;
	%END;
%MEND add_year_data;
%add_year_data();
DATA &pool_table.(drop = assets2009--research2014);
	SET tmp;
RUN;


/** Step3: 计算区分度指标 */
DATA &pool_table.;
	SET &pool_table.;
	IF revenue0+revenue1+revenue2>0 THEN
		research_pct = (research0+research1+research2)/(revenue0+revenue1+revenue2);
	ELSE research_pct = .;
	IF assets1>0 AND assets2 > 0 THEN 
		assets_pct = 0.5*(assets0/assets1+assets1/assets2)-1;
	ELSE assets_pct = .;
	IF revenue1 >0 AND revenue2 > 0 THEN 
		revenue_pct = 0.5*(revenue0/revenue1+revenue1/revenue2)-1;
	ELSE revenue_pct = .;
	IF not missing(revenue0) AND revenue0 > 0 THEN DO;
		IF revenue0 < 50000000 AND research_pct >= 0.06 THEN re_mark = 1;
		ELSE IF revenue0 >=50000000 AND research0 < 200000000 AND research_pct >= 0.04 THEN re_mark =1;
		ELSE IF research0 >= 200000000 AND research_pct >= 0.03 THEN re_mark = 1;
		ELSE re_mark = 0;
	END;
	ELSE re_mark = -1;
	IF assets_pct >0 AND revenue_pct >0 THEN as_mark = 1;
	ELSE IF not missing(assets_pct) AND not missing(revenue_pct) THEN as_mark = 0;
	ELSE as_mark = -1;
RUN;

/*DATA tt;*/
/*	SET &pool_table.;*/
/*	IF research_pct = 0 OR missing(research_pct);*/
/*RUN;*/

/** 共3个异常值。天方药业退市。均胜电子缺少2010年的资产负债表数据 */
/*DATA tt;*/
/*	SET &pool_table.;*/
/*	IF as_mark = -1 OR re_mark = -1;*/
/*RUN;*/


/*** Step4: 剔除样本1：research = 0 或者缺乏reserach_pct的样本(20只股票) */
DATA &pool_table.;
	SET &pool_table.;
	IF research_pct <= 0 OR missing(research_pct) THEN delete;
RUN;


/*** Step5: 统计结果 ***/
/** !!统计(输出) **/

/*PROC SQL;*/
/*	CREATE TABLE stat2 AS*/
/*	SELECT year, count(1) AS nobs, sum(is_in) AS in_nobs, */
/*		sum(1-is_in) AS not_in_nobs, */
/*		sum(is_in*(bk="主板")) AS zb_in,*/
/*		sum(is_in*(bk="中小企业板")) AS zxb_in,*/
/*		sum(is_in*(bk="创业板")) AS cyb_in,*/
/*		sum((bk="主板")) AS zb,*/
/*		sum((bk="中小企业板")) AS zxb,*/
/*		sum((bk="创业板")) AS cyb*/
/*	FROM &pool_table.*/
/*	GROUP BY year;*/
/*QUIT;*/

/** 行业分布*/
%get_sector_info(stock_table=&pool_table., mapping_table=fg_wind_sector, output_stock_table=&pool_table.);
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, indus_name, count(1) AS nobs
	FROM &pool_table.
	GROUP BY indus_name, end_Date;
QUIT;
PROC TRANSPOSE DATA = stat OUT = stat(drop = _NAME_);
	BY indus_name;
	VAR nobs;
	ID end_Date;
RUN;

%cal_dist(input_table=&pool_table., by_var=indus_name, cal_var=research_pct, out_table=stat);
%cal_dist(input_table=&pool_table., by_var=year, cal_var=research_pct, out_table=stat);
%cal_dist(input_table=&pool_table., by_var=year indus_name, cal_var=research_pct, out_table=stat);


