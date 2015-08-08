/*** 计算各个因子 **/
/** 富国因子针对确认后的股票池进行操作 */
/** 生成几个文件:
(0) stock_pool_raw: 富国原始因子
(1) stock_pool_z：标准化后结果
(2) stock_pool_zn：按照原始行业中性化结果  / stock_pool_zn2：TMT进行二级行业拆分后，中性化结果
(3) stock_pool_znw / stock_pool_znw2：进行winsorize后的结果
(4) stock_pool_znw_t / stock_pool_znw2_t：各个小分类和最终tot得分 (此处,tot得分进行标准化和中性化。其余小项得分未经该处理)
***/ 


%LET pool_table = subdata4_filter;


/** Step1: 将股票池扩展为每个月底 */
%get_month_date(busday_table=busday, start_date=15jun2012, end_date=30jun2015, 
	rename=end_date, output_table=month_busdate, type=1);
PROC SQL;
	CREATE TABLE adjust_busdate AS
	SELECT distinct end_date
	FROM &pool_table.
	ORDER bY end_Date;
QUIT;
%adjust_date_to_mapdate(rawdate_table=month_busdate, mapdate_table=adjust_busdate, 
		raw_colname=end_date, map_colname=end_date, output_table=month_busdate,
		is_backward=1, is_included=1);
PROC SQL;
	CREATE TABLE stock_pool AS
	SELECT A.end_date, B.stock_code, B.research_pct
	FROM month_busdate A LEFT JOIN &pool_table. B
	ON A.map_end_date = B.end_date
	ORDER BY A.end_date, B.stock_code;
QUIT;

/** Step2: 提取富国因子原始值 */

PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.stock_code AS stock_code_a, A.end_date AS end_Date_a, B.*
	FROM stock_pool A LEFT JOIN product.fg_raw_score B
	ON A.end_date = datepart(B.end_Date) AND A.stock_code = B.stock_code
	ORDER BY A.end_Date, A.stock_code;
QUIT;
/* 提取行业信息 */
PROC SQL;
	CREATe TABLE tmp2 AS
	SELECT A.*,
		B.o_code, B.o_name, B.v_code, B.v_name
	FROM tmp A LEFT JOIN bk.fg_wind_sector B
	ON A.sector_date = B.end_Date AND A.stock_code = B.stock_code
	ORDER BY end_date, stock_code;
QUIT;

DATA stock_pool_raw;
	SET tmp2(drop = stock_code_a end_date_a sector_date);
	end_date = datepart(end_date);
	FORMAT end_Date yymmdd10.;
RUN;
/** 将TMT行业进行拆分。*/
DATA stock_pool_raw;
	SET stock_pool_raw;
	IF o_name = "TMT" THEN indus_name = v_name;
	ELSE indus_name = o_name;
RUN;
/* backup */
/*DATA product.stock_pool_raw;*/
/*	SET stock_pool_raw;*/
/*RUN;*/

/** 补充：统计行业分布情况 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, indus_name, count(1) AS nobs
	FROM stock_pool_raw
	WHERE year(end_date) >= 2012 AND month(end_date)=6 AND o_name = "TMT"
	GROUP BY indus_name, end_date;
QUIT;
PROC TRANSPOSE DATA = stat prefix = Y OUT = stat;
	BY indus_name;
	ID end_date;
	VAR nobs;
RUN;

/** Step3: 标准化因子 */
%normalize_multi_score(input_table=stock_pool_raw, output_table=stock_pool_z, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));

/** Step4A: 一般因子，按照一级行业进行中性化 */
/** 估值因子PB/PE类进行的是二级中性化 */

%neutralize_multi_score(input_table=stock_pool_z, 
	output_table=stock_pool_zn, group_name=o_code, 
		exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME", 
			"V_PE_T01", "V_PE_T12", "V_PE_TTM", "V_PEE", "V_PFV", "V_PS_TTM","V_PB_T01", "V_PB_T12", "V_PB_TTM",  "V_PCF_TTM"));

%neutralize_multi_score(input_table=stock_pool_zn, 
	output_table=stock_pool_zn, group_name=v_code, 
		include_list=("V_PE_T01", "V_PE_T12", "V_PE_TTM", "V_PEE", "V_PFV", "V_PS_TTM","V_PB_T01", "V_PB_T12", "V_PB_TTM",  "V_PCF_TTM"), type=2);

/** Step4B: 一般因子按照新定义行业(indus_name)进行中性化。估值因子仍按照二级行业进行中性化 */
%neutralize_multi_score(input_table=stock_pool_z, 
	output_table=stock_pool_zn2, group_name=indus_name, 
		exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME", 
			"V_PE_T01", "V_PE_T12", "V_PE_TTM", "V_PEE", "V_PFV", "V_PS_TTM","V_PB_T01", "V_PB_T12", "V_PB_TTM",  "V_PCF_TTM"));

%neutralize_multi_score(input_table=stock_pool_zn2, 
	output_table=stock_pool_zn2, group_name=v_code, 
		include_list=("V_PE_T01", "V_PE_T12", "V_PE_TTM", "V_PEE", "V_PFV", "V_PS_TTM","V_PB_T01", "V_PB_T12", "V_PB_TTM",  "V_PCF_TTM"), type=2);


/** Step5: winsorize */
%winsorize_multi_score(input_table=stock_pool_zn, 
	output_table=stock_pool_znw, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"), type=1, upper=3, lower = -3);
%winsorize_multi_score(input_table=stock_pool_zn2, 
	output_table=stock_pool_znw2, 
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"), type=1, upper=3, lower = -3)

/** Step6: 计算分类因子得分 */
%LET score_table = stock_pool_znw2;

/** 倒置得分 */
PROC SORT DATA = &score_table.(drop = fmv_sqr) OUT =&score_table._t ;
	BY end_date stock_code o_code o_name v_code v_name indus_name;
RUN;
PROC TRANSPOSE DATA = &score_table._t OUT = &score_table._t(rename = (_NAME_=factor_name col1=factor_value) drop = _LABEL_);
	BY end_date stock_code o_code o_name v_code v_name indus_name;
	VAR crateps_gg--Gro_eps_t01_abs;
RUN;
/** 匹配o_code和group_code */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.group_code AS fsector, B.group_name AS fsector_name
	FROM &score_table._t A LEFT JOIN tinysoft.fg_sector_grp B
	ON A.o_code = B.o_code
	ORDER BY A.end_date, A.stock_code;
QUIT;

/**　匹配权重　*/
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*, B.fgroup, B.weight AS factor_weight
	FROM tmp A LEFT JOIN zhuti.fg_score_weight B
	ON A.fsector = B.fsector AND upcase(A.factor_name) = upcase(B.fsignal)
	ORDER BY A.end_date,  A.stock_code, A.o_code;
QUIT;
/** 未匹配权重的，认为权重为0 */
DATA &score_table._t;
	SET tmp2;
	IF missing(factor_weight) THEN factor_weight = 0;
RUN;

/** 计算分类因子和综合因子得分 */
PROC SQL;
	CREATE TABLE tmp1 AS
	SELECT end_date, stock_code, fgroup, 
	coalesce(sum(factor_weight*factor_value)/sum(factor_weight),0) AS score
	FROM &score_table._t
	WHERE not missing(fgroup)
	GROUP BY end_date, stock_code, fgroup;
QUIT;
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT end_date, stock_code, "tot" AS fgroup,
		coalesce(sum(factor_weight*factor_value)/sum(factor_weight),0) AS score
	FROM &score_table._t
	GROUP BY end_Date, stock_code;
QUIT;

DATA &score_table._t;
	SET tmp1 tmp2;
RUN;
PROC SORT DATA = &score_table._t;
	BY end_date stock_code;
RUN;
PROC TRANSPOSE DATA = &score_table._t OUT = &score_table._t(drop = _NAME_);
	BY end_date stock_code;
	ID fgroup;
	VAR score;
RUN;
/** 对tot进行标准化和winsorize */
/* 取自由流通市值 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.fmv_sqr
	FROM &score_table._t A LEFT JOIN product.fg_raw_score B
	ON A.end_date = datepart(B.end_Date) AND A.stock_code = B.stock_code
	ORDER BY A.end_Date, A.stock_code;
QUIT;
DATA &score_table._t;
	SET tmp;
RUN;
%normalize_single_score(input_table=&score_table._t, colname=tot, output_table=&score_table._t, is_replace = 1);
%winsorize_single_score(input_table=&score_table._t, colname=tot, output_table=&score_table._t, upper=3, lower = -3, is_replace = 1);


/************ 1-专利相关因子(以发布为准) */

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
	ON A.map_pub_date = B.date
	WHERE map_pub_date >= "15dec2010"d;
QUIT;
DATA daily_zl;
	SET tmp;
RUN;

DATA daily_zl;
	SET daily_zl;
	zl_score = nobs_fm*1 + nobs_sy*0.2 + nobs_wg*0.5;
	fm_all = nobs_fm + nobs2;
RUN;

PROC SQL;
	CREATE TABLE zl_factor AS
	SELECT A.*,
	coalesce(B.nobs,0) AS nobs,
	coalesce(B.nobs1,0) AS nobs1, /** 第一次发布 */
	coalesce(B.nobs2,0) AS nobs2,
	coalesce(B.nobs_fm,0) AS nobs_fm,   /** 词语不同的得分 */
	coalesce(B.nobs_sy,0) AS nobs_sy,
	coalesce(B.nobs_wg,0) AS nobs_wg,
	coalesce(B.zl_score,0) AS zl_score,
	coalesce(B.fm_all,0) AS fm_all
	FROM stock_pool A LEFT JOIN daily_zl B
	ON A.stock_code = B.stock_code AND A.end_date = B.map_pub_date
	ORDER BY A.end_Date, A.stock_code;
QUIT;


PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.month_id
	FROM zl_factor A LEFT JOIN month_busdate B
	ON A.end_date = B.date;
QUIT;
DATA zl_factor2;
	SET tmp;
RUN;

/** 包含该月 */
%MACRO past_var(varname, varname_mdf, start_month, end_month, is_include=1); 
	%IF %SYSEVALF(&is_include.=1) %THEN %DO;	
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, coalesce(C.&varname.,0) AS &varname._tmp
			FROM zl_factor2 A 
			LEFT JOIN  daily_zl C
			ON A.stock_code = C.stock_code AND A.month_id - &end_month. <= C.month_id <= A.month_id - &start_month. 
			ORDER BY A.stock_code, A.end_date;
		QUIT;
		/** 如果缺失，则认为该月份为0。因为tmp中会没有该记录，所以求均值限求和在除以月份数，而不是直接使用mean*/
		PROC SQL;
			CREATE TABLE tmp2 AS
			SELECT A.*, B.&varname_mdf.
			FROM zl_factor2 A
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
			FROM zl_factor2 A 
			LEFT JOIN  daily_zl C
			ON A.stock_code = C.stock_code AND A.month_id - &end_month. <= C.month_id < A.month_id - &start_month. 
			ORDER BY A.stock_code, A.end_date;
		QUIT;
		PROC SQL;
			CREATE TABLE tmp2 AS
			SELECT A.*, B.&varname_mdf.
			FROM zl_factor2 A
			LEFT JOIN
			(SELECT stock_code,month_id, sum(&varname._tmp)/(&end_month.-(&start_month.)) AS &varname_mdf.
			FROM tmp
			GROUP BY stock_code, month_id) B
			ON A.stock_code = B.stock_code AND A.month_id = B.month_id
			ORDER BY A.stock_code, A.month_id;
		QUIT;
	%END;
	DATA zl_factor2;
		SET tmp2;
		IF missing( &varname_mdf.) THEN  &varname_mdf. = 0;
	RUN;

		
	PROC SQL;
		DROP TABLE tmp, tmp2;
	QUIT;
%MEND past_var;


/*%past_var(varname = nobs, start_month=0, end_month=3, is_include=0);*/
%past_var(varname = nobs_wg, varname_mdf = nobs_prev0, start_month=0, end_month=0, is_include=1);
%past_var(varname = nobs_wg, varname_mdf = nobs_prev1, start_month=1, end_month=1, is_include=1);
%past_var(varname = nobs_wg, varname_mdf = nobs_prev2, start_month=2, end_month=2, is_include=1);
%past_var(varname = nobs_wg, varname_mdf = nobs_prev3, start_month=3, end_month=3, is_include=1);
%past_var(varname = nobs_wg, varname_mdf = nobs_prev4, start_month=4, end_month=4, is_include=1);
%past_var(varname = nobs_wg, varname_mdf = nobs_prev5, start_month=5, end_month=5, is_include=1);
%past_var(varname = nobs_wg, varname_mdf = nobs_prev6, start_month=6, end_month=6, is_include=1);


%past_var(varname = nobs1, varname_mdf = nobs_prev0, start_month=0, end_month=0, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev1, start_month=1, end_month=1, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev2, start_month=2, end_month=2, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev3, start_month=3, end_month=3, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev4, start_month=4, end_month=4, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev5, start_month=5, end_month=5, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev6, start_month=6, end_month=6, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev7, start_month=7, end_month=7, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev8, start_month=8, end_month=8, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev9, start_month=9, end_month=9, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev10, start_month=10, end_month=10, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev11, start_month=11, end_month=11, is_include=1);
%past_var(varname = nobs1, varname_mdf = nobs_prev12, start_month=12, end_month=12, is_include=1);

%past_var(varname = fm_all, varname_mdf = nobs_prev0, start_month=0, end_month=0, is_include=1);
%past_var(varname = fm_all, varname_mdf = nobs_prev1, start_month=1, end_month=1, is_include=1);
%past_var(varname = fm_all, varname_mdf = nobs_prev2, start_month=2, end_month=2, is_include=1);
%past_var(varname = fm_all, varname_mdf = nobs_prev3, start_month=3, end_month=3, is_include=1);
%past_var(varname = fm_all, varname_mdf = nobs_prev4, start_month=4, end_month=4, is_include=1);
%past_var(varname = fm_all, varname_mdf = nobs_prev5, start_month=5, end_month=5, is_include=1);
%past_var(varname = fm_all, varname_mdf = nobs_prev6, start_month=6, end_month=6, is_include=1);


/** 对research_pct进行标准化和winsorize */
/* 取自由流通市值 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.fmv_sqr
	FROM zl_factor2 A LEFT JOIN product.fg_raw_score B
	ON A.end_date = datepart(B.end_Date) AND A.stock_code = B.stock_code
	ORDER BY A.end_Date, A.stock_code;
QUIT;
DATA zl_factor3;
	SET tmp;
RUN;

%normalize_single_score(input_table=zl_factor3, colname=research_pct, output_table=zl_factor3, is_replace = 1);
%winsorize_single_score(input_table=zl_factor3, colname=research_pct, output_table=zl_factor3, upper=3, lower = -3, is_replace = 1);


%normalize_single_score(input_table=zl_factor3, colname=nobs1, output_table=zl_factor3, is_replace = 1);
%winsorize_single_score(input_table=zl_factor3, colname=nobs1, output_table=zl_factor3, upper=3, lower = -3, is_replace = 1);


/** 统计1: 每月有多少有专利数据*/
/** 2014年之前占比能在40%以上，但2014年6月以后，占比只能在20%左右 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT end_Date, count(1) AS nobs,
	sum(nobs>0) AS nobs_zl,
	sum(nobs1>0) AS nobs_zl1,
	sum(zl_score+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>0) AS nobs_valid
	FROM zl_factor2
	GROUP BY end_date;
QUIT;

/** 统计2: 每月有多少有专利数据*/
%cal_dist(input_table=zl_factor2, by_var=end_date, cal_var=nobs_dif, out_table=stat);






/************ 富国结果与文汉结果作对比 */
/**　完全一致　*/
%LET fname = tot;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, 
		A.stock_code AS stock_code_a,
		A.&fname. AS &fname._a,
		B.stock_code AS stock_code_b,
		B.&fname. AS &fname._b,
		abs(A.&fname. - B.&fname.) AS dif
	FROM stock_pool_znw_t A LEFT JOIN fgtest.fg_res_portscore B
	ON A.stock_code = B.stock_code AND A.end_date = datepart(B.sector_date)
	ORDER BY A.end_date, A.stock_code;
QUIT;
DATA tt;
	SET tmp;
	IF dif >= 0.0001;
RUN;


