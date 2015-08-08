/*** 完整过程 **/

/** Step1: 专利数据 **/
/** 生成: zl_change */

PROC SQL;
	CREATE TABLE zlsj AS
	SELECT 股票代码 AS stock_code, 
			申请号 AS an,
			公开号 AS pn,
			申请日 AS apply_date,
			公开日 AS pub_date,
			名称 AS name,
			主分类号 AS main_class,
			申请人 AS applicant,
			发明人 AS inventor,
			专利类型 AS ptype,
			table_sn AS table_sn, 
/*			摘要 AS abstract,*/
			法律状态 AS law_date,
			页数 AS pages,
			专利权状态 AS status,
			族号 AS group_code,
			申请国代码 AS country
	FROM fgtest.test_zl3
	ORDER BY stock_code, pn;
QUIT;
DATA product.zlsj(rename = (apply_date2 = apply_date pub_date2 = pub_date));
	SET zlsj;
	apply_date = trim(compress(apply_date,"."));
	pub_date = trim(compress(pub_date,"."));
	apply_date2 = input(apply_date, yymmdd8.);
	pub_date2 = input(pub_date, yymmdd8.);
	FORMAT apply_date2 pub_date2 yymmdd10.;
	drop apply_date pub_date;
	tail = substr(pn, length(pn),1);
	IF tail IN ("B","C") THEN my_status = 2; /** 第二次公开 */
	ELSE my_status = 1;
	IF ptype IN ("1","8") THEN my_type = 1;  /** 发明专利 */
	ELSE IF ptype IN ("2","9") THEN my_type = 2; /** 实用新型 */
	ELSE my_type = 3; /* 外观设计 */
RUN;

DATA zlsj_subset;
	SET product.zlsj;
	WHERE pub_date >= "15dec2008"d;
RUN;
%get_month_date(busday_table=busday, start_date=15dec2008, end_date=30jun2015, rename=date, output_table=month_busdate, type=1);
/*%adjust_date_modify(busday_table=busday , raw_table=zlsj_subset ,colname=pub_date,  output_table=zlsj_subset, is_forward = 1);*/
%adjust_date_to_mapdate(rawdate_table=zlsj_subset, mapdate_table=month_busdate, 
	raw_colname=pub_date, map_colname=date, 
	output_table=zlsj_subset,is_backward=0, is_included=1);

PROC SQL;
	CREATE TABLE daily_zl AS
	SELECT stock_code, map_pub_date, count(1) AS nobs, 
		sum(my_status=1) AS nobs1, sum(my_status=2) AS nobs2,
		sum(my_type=1 AND my_status=1) AS nobs_fm,
		sum(my_type=2 AND my_status=1) AS nobs_sy,
		sum(my_type=3 AND my_status=1) AS nobs_wg
	FROM zlsj_subset
	GROUP BY stock_code, map_pub_Date;
QUIT;

DATA daily_zl;
	SET daily_zl;
	year = year(map_pub_date);
RUN;
PROC SORT DATA = daily_zl;
	BY descending nobs;
RUN;
PROC SORT DATA = daily_zl;
	BY year;
RUN;

DATA daily_zl_subset;
	SET daily_zl;
	IF map_pub_date >= "15dec2010"d;
RUN;
PROC SORT DATa = daily_zl_subset;
	BY year;
RUN;

DATA zlsj_subset_expand;
	SET zlsj_subset;
	IF map_pub_date >= "15dec2010"d;
	/** 假设1 **/
	IF apply_date <= mdy(6,30,year(apply_date)) THEN att_year = year(apply_date)-1;
	ELSE att_year = year(apply_date);
	/** 假设2 **/
/*	att_year = year(apply_date);*/
	/** 数据可得，要求在下一年的6-30之前能得到 */
	IF map_pub_date < mdy(7,1,att_year+1) THEN is_get = 1;
	ELSE is_get = 0;
RUN;

/** 按照年份(att_year)统计个股专利情况 */
PROC SQL;
	CREATE TABLE zl_change AS
	SELECT stock_code, att_year, att_year+1 AS year,
		sum(is_get=1) AS n_get,  /** 以此作为筛选标准 **/
		sum(is_get=0) AS n_notget,
		sum(is_get=1 and my_status=1) AS n_first_get,
		sum(is_get=1 and my_status=2) AS n_second_get,
		sum(is_get=1 and my_status=1 and my_type=1) AS n_fm,
		sum(is_get=1 and my_status=1 and my_type=2) AS n_sy,
		sum(is_get=1 and my_status=1 and my_type=3) AS n_wg
	FROM zlsj_subset_expand
	GROUP BY stock_code, att_year;
QUIT;

PROC SQL;
	DROP TABLE zlsj_subset, zlsj_subset_expand, daily_zl, daily_zl_subset;
QUIT;
