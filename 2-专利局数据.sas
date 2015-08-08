/*** 专利局数据 */
/*** 数据来源：
(1) 专利数据: fgtest.test_zl3(该表格内已按照stock_code进行归类) --> zl_change(其中att_year+1才是股票池生成的日期)
生成zl_change的过程:
product.zlsj --> zljs_subset --> zljs_subset_expand --> zl_change（年频）
生成create_zl_month的过程：
create_zl --> create_zl_month: （月频）
(2) 高新技术股票池: gxjs_stock_pool (来自: gxjs_stock_pool经过股票池过滤后的结果)
**/

/** 核心步骤:
(1) 一些专利的统计数据
(2) 与gxjs_stock_pool进行合并，生成union_pool
***/

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


/** (!!输出) 统计1：每个月新增的专利数量 */
/** 发明专利占有：70% */
PROC SQL;
	CREATE TABLE stat AS
	SELECT year(pub_date) AS year, month(pub_date) AS month, 
	sum(my_type=1 and my_status=2) AS fm2,
	sum(my_type=1 and my_status=1) AS fm1,
	sum(my_type=2 and my_status=1) AS sy1,
	sum(my_type=3 and my_status=1) AS wg1,
	count(1) AS nobs
	FROM product.zlsj
	GROUP BY year, month;
QUIT;

/** (!!输出）统计2：统计每月有专利数据的上市公司数量 */
/** 平均每个月有130家上市公司发布专利数据 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT year(pub_date) AS year, month(pub_date) AS month, 
	count(distinct stock_code) AS nstock
	FROM product.zlsj
	WHERE pub_date >= "31dec2010"d
	GROUP BY year, month;
QUIT;


/** (必要!!) 统计3：上市公司距离最近一次发布的时间间隔 */
/*** 分析结果：大多数上市公司在过去相邻月份已经会有专利数据 **/
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
PROC SORT DATA = month_busdate;
	BY date;
RUN;
DATA month_busdate;
	SET month_busdate;
	month_id = _N_;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.month_id
	FROM daily_zl A LEFT JOIN month_busdate B
	ON A.map_pub_date = B.date;
QUIT;
PROC SQL;
	CREATE TABLE daily_zl AS
	SELECT A.*, B.month_id AS prev_month_id
	FROM tmp A LEFT JOIN tmp B
	ON A.stock_code = B.stock_code AND A.month_id > B.month_id
	WHERE A.map_pub_date >= "15dec2010"d
	GROUP BY A.stock_code, A.month_id
	HAVING B.month_id = max(B.month_id);
QUIT;

DATA daily_zl;
	SET daily_zl;
	dif = month_id - prev_month_id;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT dif, count(1) AS nobs
	FROM daily_zl
	GROUP BY dif;
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

/** (!!输出) 统计4：上市公司每年发布的专利数量 */
/** 有部分股票，单年的专利数量非常大，如中兴通讯(000063)。其余大多数股票数量都在3左右 */
PROC UNIVARIATE DATA = daily_zl_subset NOPRINT;
	BY year;
	VAR nobs;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;

/** 统计5: 不同行业内，个股每月发布的专利数量 */
/** 集中的一级行业有: TMT / 电气设备 / 机械 /建筑建材 */
DATA daily_zl_subset;
	SET daily_zl_subset;
	end_date = map_pub_date;
	FORMAT end_date yymmdd10.;
RUN;
%get_sector_info(stock_table=daily_zl_subset, mapping_table=fg_wind_sector, output_stock_table=daily_zl_subset);
PROC SORT DATA = daily_zl_subset;
	BY indus_name;
RUN;

PROC UNIVARIATE DATA = daily_zl_subset NOPRINT;
	BY indus_name;
	VAR nobs;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;
PROC SQL;
	CREATE TABLE stat AS
	SELECT indus_name, count(distinct stock_code) AS nstock
	FROM daily_zl_subset
	GROUP BY indus_name
	ORDER BY nstock desc;
QUIT;


/** (!!输出) 统计6: 申请日和公开日之间的时间差异  */
/** 发明，实用新型和外观的第一次公开日期，距离申请日的中位数在180-200天之间*/
/** 大概跨越半年的时间 */
DATA tt;
	SET zlsj_subset;
	dif = pub_date - apply_date;
	IF map_pub_date >= "15dec2010"d;
RUN;
PROC SORT DATA = tt;
	BY ptype my_status;
RUN;
PROC UNIVARIATE DATA = tt NOPRINT;
	BY ptype my_status;
	VAR dif;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;



/** （必要!!!）统计7：定义日期 */
/** 提供两种假设：
(假设1) 研发周期为半年：申请日在[去年7月1日，今年6月30(map_pub_date)]之间的认为是去年研发费用的成果 -> 以6/30作为分界线
(假设2) 研发周期成果认为是即时的：申请日在[去年1月1日，去年12月31日]之间的认为是去年研发费用的成果 -> 以12/31作为分界线 
定义转换率 = 期间专利数 / 研发费用
另外，因为科研费用在6月底调整，所以如果在同样的时点计算转换率，要求数据在今年6/30日之前可得的，才能计算。
****/

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

/** 统计8: 以上市公司为单位统计三种类型的专利覆盖情况　*/
/** 不考虑数据是否可得,但剔除了发明专利授权(my_status=2) */
PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all
	FROM zlsj_subset_expand
	GROUP BY year, stock_code;
QUIT;
/** 专利的中位数，2011年以来大致在10-15家左右 */
%cal_dist(input_table=stat, by_var=year, cal_var=n_all, out_table=stat2);

/** 考虑可得性 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all,
		count(1) AS n_all2
	FROM zlsj_subset_expand
	WHERE is_get = 1
	GROUP BY year, stock_code;
QUIT;
/** 专利的中位数，2011年以来大致在10-15家左右 */
%cal_dist(input_table=stat, by_var=year, cal_var=n_all, out_table=stat2);
%cal_dist(input_table=stat, by_var=year, cal_var=n_all2, out_table=stat2);


/** 为了标注上市公司专利情况,考察两类样本:
(1) 全样本
(2) 总专利数>=10
***/
%LET zl_var = n_fm;
DATA stat;
	SET stat;
	IF n_all = 0 THEN delete;
	IF n_all >=10 THEN mark = 1;
	ELSE mark = 0;
	IF &zl_var./n_all = 1 THEN g = 1;
	ELSE IF &zl_var./n_all >0.8 THEN g =2;
	ELSE IF &zl_var./n_all >0.5 THEN g= 3;
	ELSE IF  &zl_var./n_all > 0 THEN g = 4;
	ELSE g = 5;
RUN;
PROC SQL;
	CREATE TABLE stat2 AS
	SELECT year, g, count(1) AS nobs
	FROM stat
/*	WHERE mark = 1*/
	GROUP BY year, g;
QUIT;
PROC TRANSPOSE DATA = stat2 prefix = g OUT = stat2(drop = _NAME_);
	BY year;
	ID g;
	WHERE year >= 2012;
	VAR nobs;
RUN;


/** 统计9: 统计三种类型的上市公司行业分布　*/
PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all
	FROM zlsj_subset_expand
	GROUP BY year, stock_code;
QUIT;
DATA stat;
	SET stat;
	IF n_all = 0 THEN delete;
	IF n_all > 0 THEN DO;
		IF n_fm/n_all > 0.5 THEN g = 1;
		ELSE IF n_sy/n_all > 0.5 THEN g = 2;
		ELSE IF n_wg/n_all > 0.5 THEN g = 3;
		ELSE g = 4;
	END;
	end_date = mdy(6,30,year);
	FORMAT end_date mmddyy10.;
RUN;
%get_sector_info(stock_table=stat, mapping_table=fg_wind_sector, output_stock_table=stat);

PROC SQL;
	CREATE TABLE stat2 AS
	SELECT year, g, count(1) AS nobs
	FROM stat
	WHERE year >= 2012
	GROUP BY year, g;
QUIT;
PROC TRANSPOSE DATA = stat2 prefix = g OUT = stat2(drop = _NAME_);
	BY year;
	ID g;
	VAR nobs;
RUN;

%LET g_group = 1;
PROC SQL;
	CREATE TABLE stat2 AS
	SELECT year, indus_name, count(1) AS nobs
	FROM stat
	WHERE g = &g_group. AND year >= 2012
	GROUP BY indus_name, year;
QUIT;
PROC TRANSPOSE DATA = stat2 prefix = y OUT = stat2(drop = _NAME_);
	BY indus_name;
	ID year;
	VAR nobs;
RUN;
DATA stat2(drop = i);
	SET stat2;
	ARRAY var_list(4) y2012-y2015;
	DO i = 1 TO 4;
		IF missing(var_list(i)) THEN var_list(i) = 0;
	END;
RUN; 


/** 统计10: 上市公司每年是否都有一定数量的新增专利成果　*/
 PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all
	FROM zlsj_subset_expand
	GROUP BY stock_code, year;
QUIT;
DATA stat;
	SET stat;
	IF n_all = 0 THEN delete;
RUN;
PROC TRANSPOSE DATA = stat prefix = y OUT = stat(drop = _NAME_);
	BY stock_code;
	ID year;
	VAR n_all;
	WHERE year >= 2011;
RUN;
DATA stat(drop = i);
	SET stat;
	ARRAY var_list(5) y2011-y2015;
	ARRAY var_list2(5) y2011_m y2012_m y2013_m y2014_m y2015_m;
	DO i = 1 TO 5;
		IF missing(var_list(i)) THEN var_list(i) = 0;
		IF var_list(i) > 0 THEN var_list2(i) = 1;
		ELSE var_list2(i) = 0;
	END;
RUN; 
DATA stat(drop = i);
	SET stat;
	ARRAY var_list2(4) m1 m2 m3 m4;
	ARRAY var_list(5) y2011_m y2012_m y2013_m y2014_m y2015_m;
	DO i = 2 TO 5;
		IF var_list(i-1) = 1 AND var_list(i) = 1 THEN var_list2(i-1) = 1;
		ELSE IF var_list(i-1) = 0 AND var_list(i) = 0 THEN var_list2(i-1) = 2;
		ELSE IF var_list(i-1) = 0 AND var_list(i) = 1 THEN var_list2(i-1) = 3;
		ELSE IF var_list(i-1) = 1 AND var_list(i) = 0 THEN var_list2(i-1) = 4;
	END;
RUN; 
%LET m_group = m4;
PROC SQL;
	CREATE TABLE stat2 AS
	SELECT &m_group., count(1) AS nobs
	FROM stat
	GROUP BY &m_group.;
QUIT;


/** 统计11: 上市公司以每月新增专利数量排序，及其之间的相关性(考虑构造月度因子的可能性)　*/

%MACRO cal_coef_add(coef_var, output_table);
	PROC SORT DATA = daily_zl_subset;
		BY stock_code;
	RUN;
	DATA tt;
		SET daily_zl_subset;
		KEEP map_pub_date stock_code &coef_var.;
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, coalesce(C.&coef_var.,0) AS &coef_var., 		
		year(A.map_pub_date)*10000+month(A.map_pub_date)*100+day(A.map_pub_date) AS date_num,
		A.map_pub_date AS end_Date FORMAT yymmdd10.
		FROM
		(
		SELECT map_pub_date, stock_code 
		FROM
		(SELECT distinct map_pub_date FROM tt) ,
		(SELECT distinct stock_code FROM tt) 
		) A 
		LEFT JOIN tt C
		ON A.map_pub_date = C.map_pub_date AND A.stock_code = C.stock_code
		ORDER BY A.stock_code, date_num;
	QUIT;
	PROC TRANSPOSE DATA = tmp prefix = d OUT = results;
		BY stock_code;
		ID date_num;
		VAR &coef_var.;
	RUN;
	
	%LET date_list = .;
	%LET ndate =.;
	%gen_macro_var_list(input_table=tmp, var_name=date_num, var_macro=date_list, nobs_macro=ndate);
	%put &date_list.;
	%put &ndate.;
	%DO i = 2 %TO &ndate.;
		%LET j = %sysevalf(&i.-1);
		%LET prev_month = %SCAN(&date_list., &j., " ");
		%LET cur_month = %SCAN(&date_list., &i., " ");
		DATA results;
			SET results;
			end_date = &cur_month.;
		RUN;
		%cal_coef(data=results, var1=d&prev_month., var2=d&cur_month., output_s = corr_s, output_p = corr_p);
		PROC SQL;
			CREATE TABLE stat AS
			SELECT A.end_Date, A.p_ic, B.s_ic
			FROM corr_p A JOIN corr_s B
			ON A.end_date = B.end_Date;
		QUIT;
		%IF %SYSEVALF(&i.=2) %THEN %DO;
			DATA &output_table.;
				SET stat;
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. stat;
			RUN;
		%END;
	%END;
	PROC SQL;
		DROP TABLE stat, tmp, results, corr_s, corr_p;
	QUIT;

%MEND cal_coef_add;
%cal_coef_add(nobs1, zl_results);




/************* 创新能力的衡量(月度) ***************/

%get_month_date(busday_table=busday, start_date=15dec2011, end_date=30jun2015, rename=end_date, output_table=month_busdate, type=1);
DATA create_zl;
	SET month_busdate;
	IF end_date <= mdy(6,30,year(end_date)) THEN report_year = year(end_date)-2;
	ELSE report_year = year(end_date)-1;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.report_year,
		B.stock_code,
		B.pn,
		B.apply_date,
		B.pub_date,
		B.my_status,
		B.my_type,
		B.map_pub_date,
		B.status
	FROM create_zl A LEFT JOIN zlsj_subset B
	ON B.apply_date <= mdy(6,30,A.report_year+1)
	  AND B.apply_date > mdy(6,30,A.report_year)
      AND pub_date < end_date
	ORDER BY A.end_date, B.apply_date;
QUIT;
DATA create_zl;
	SET tmp;
RUN;
 
/** 统计每个月度，汇总的报告年份的专利数量(随着时间的推移，会发生一定的变化) */
PROC SQL;
	CREATE TABLE create_zl_month AS
	SELECT end_date, stock_code,report_year,
		sum(my_status=1 and my_type=1 and status ~="20") AS n_fm,
		sum(my_status=1 and my_type=2 and status ~= "20") AS n_sy,
		sum(my_status=1 and my_type=3 and status ~= "20") AS n_wg,
		sum(my_status=1 and status ~= "20") AS n_all,
		sum(my_status=2 and status ~= "20") AS n_fm_pub
	FROM create_zl
	GROUP BY end_date, stock_code, report_year
	ORDER BY stock_code, end_Date;
QUIT;

/** 与财报数据相连 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.*
	FROM create_zl_month A LEFT JOIN product.report_data B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
%MACRO add_report_data();
	%DO i = 2009 %TO 2014;
		DATA tmp;
			SET tmp;
			IF report_year = &i. THEN DO;
				research = research%sysevalf(&i.);
				assets = assets%sysevalf(&i.);
				revenue = revenue%sysevalf(&i.);
			END;
		RUN;
	%END;
%MEND add_report_data;
%add_report_data();
DATA create_zl_month(drop = assets2009--research2014);
	SET tmp;
	IF not missing(revenue) AND revenue ~=0 THEN DO;
		zl_value_fm = n_fm*100000000/revenue;
		zl_value_all = n_all*100000000/revenue;
	END;
	ELSE DO;
		zl_value_fm = .;
		zl_value_all = .;
	END;
RUN;
%cal_dist(input_table=create_zl_month, by_var=end_date, cal_var=zl_value_fm, out_table=stat);
%cal_dist(input_table=create_zl_month, by_var=end_date, cal_var=zl_value_all, out_table=stat);

/** 覆盖的股票数量 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT distinct stock_code
	FROM create_zl_month;
QUIT;



/************* 事件冲击(月度) ***************/

DATA zlsj_subset;
	SET product.zlsj;
	WHERE pub_date >= "15dec2008"d;
RUN;
%get_month_date(busday_table=busday, start_date=15dec2008, end_date=30jun2015, rename=date, output_table=month_busdate, type=1);
%adjust_date_to_mapdate(rawdate_table=zlsj_subset, mapdate_table=month_busdate, 
	raw_colname=pub_date, map_colname=date, 
	output_table=zlsj_subset,is_backward=0, is_included=1);

PROC SQL;
	CREATE TABLE zl_factor AS
	SELECT stock_code, map_pub_date, count(1) AS nobs, 
		sum(my_status=1 and my_type=1 and status ~="20") AS n_fm,
		sum(my_status=1 and my_type=2 and status ~= "20") AS n_sy,
		sum(my_status=1 and my_type=3 and status ~= "20") AS n_wg,
		sum(my_status=1 and status ~= "20") AS n_all,
		sum(my_status=2 and status ~= "20") AS n_fm_pub,
		sum(status = "20") AS n_fail
	FROM zlsj_subset
	GROUP BY stock_code, map_pub_Date;
QUIT;
PROC SORT DATA = month_busdate;
	BY date;
RUN;
DATA month_busdate;
	SET month_busdate;
	month_id = _N_;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.month_id, B.date AS end_date FORMAT yymmdd10.
	FROM zl_factor A LEFT JOIN month_busdate B
	ON A.map_pub_date = B.date
	WHERE map_pub_date >= "15dec2010"d;
QUIT;
DATA zl_factor;
	SET tmp;
	drop map_pub_date;
RUN;


/** 包含该月 */
%MACRO past_var(varname, varname_mdf, start_month, end_month, is_include=1); 
	%IF %SYSEVALF(&is_include.=1) %THEN %DO;	
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, coalesce(C.&varname.,0) AS &varname._tmp
			FROM zl_factor A 
			LEFT JOIN zl_factor C
			ON A.stock_code = C.stock_code AND A.month_id - &end_month. <= C.month_id <= A.month_id - &start_month. 
			ORDER BY A.stock_code, A.end_date;
		QUIT;
		/** 如果缺失，则认为该月份为0。因为tmp中会没有该记录，所以求均值限求和在除以月份数，而不是直接使用mean*/
		PROC SQL;
			CREATE TABLE tmp2 AS
			SELECT A.*, B.&varname_mdf.
			FROM zl_factor A
			LEFT JOIN
			(SELECT stock_code,month_id, sum(&varname._tmp)/(&end_month.-(&start_month.)+1) AS  &varname_mdf.
			FROM tmp
			GROUP BY stock_code, month_id) B
			ON A.stock_code = B.stock_code AND A.month_id = B.month_id
			ORDER BY A.stock_code, A.month_id;
		QUIT;
	%END;

	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, coalesce(C.&varname.,0) AS &varname._tmp
			FROM zl_factor A 
			LEFT JOIN zl_factor C
			ON A.stock_code = C.stock_code AND A.month_id - &end_month. <= C.month_id < A.month_id - &start_month. 
			ORDER BY A.stock_code, A.end_date;
		QUIT;
		PROC SQL;
			CREATE TABLE tmp2 AS
			SELECT A.*, B.&varname_mdf.
			FROM zl_factor A
			LEFT JOIN
			(SELECT stock_code,month_id, sum(&varname._tmp)/(&end_month.-(&start_month.)) AS &varname_mdf.
			FROM tmp
			GROUP BY stock_code, month_id) B
			ON A.stock_code = B.stock_code AND A.month_id = B.month_id
			ORDER BY A.stock_code, A.month_id;
		QUIT;
	%END;
	DATA zl_factor;
		SET tmp2;
		IF missing( &varname_mdf.) THEN  &varname_mdf. = 0;
	RUN;

		
	PROC SQL;
		DROP TABLE tmp, tmp2;
	QUIT;
%MEND past_var;

/** 根据需要，赋不同的专利变量 */
%LET varname = n_all;
%past_var(varname = &varname., varname_mdf = nobs_prev0, start_month=0, end_month=0, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev1, start_month=1, end_month=1, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev2, start_month=2, end_month=2, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev3, start_month=3, end_month=3, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev4, start_month=4, end_month=4, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev5, start_month=5, end_month=5, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev6, start_month=6, end_month=6, is_include=1);



/** 人工其他加工 */
DATA zl_factor;
	SET zl_factor;
	/** 根据当前月份的绝对值 */
	cur_zl = nobs_prev0;
	/** 过去三个月总量绝对值 */
	cur_zl_t3 = nobs_prev0+nobs_prev1+nobs_prev2;
	/** 过去六个月总量绝对值 */
	cur_zl_t6 = nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5;
	/* 当前月份相对过去三个月的绝对值 */
	cur_zl_dif3 = nobs_prev0 - (nobs_prev0+nobs_prev1+nobs_prev2)/3;
	/* 当前月份相对过去6个月的绝对值 */
	cur_zl_dif6 = nobs_prev0 - (nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5)/6;
	/* 过去三个月相对过去6个月的绝对值 */
	cur_zl_dif36 = (nobs_prev0+nobs_prev1+nobs_prev2)/3 - (nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5)/6;
RUN;

