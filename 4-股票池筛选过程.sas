/** 输入: union_pool + zl_change + gxsj_stock_pool中的三者之一 */
/** 考察: research_pct或zl_group分组效果的区分度 */
/*注: research_pct为连续变量。zl_group为离散变量。二者的分析模块略微有差异，注意区分。*/

/** 最终股票池的逻辑为:
(1) research_pct排名前50%
(2) 行业过滤要求：富国一级行业的股票数量>=6
***/
/** 股票池过程: union_pool --> subdata --> subdata2 --> subdata2_filter --> subdata2_filter2 */

/** 股票池的结果为: subdata2_filter2 **/




/** Step1: 计算单月或累计收益率，用于之后计算IC或分组收益 ***/
/** 月末调整*/
%LET adjust_start_date = 5may2012;   
%LET adjust_end_date = 30jun2015;

%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
	rename=end_date, output_table=adjust_busdate, type=1);

%LET ic_length = 12;

PROC SQL;
	CREATE TABLE raw_table AS
	SELECT end_date, stock_code, close*factor AS price
	FROM hqinfo
	where end_date in 
	(SELECT end_date FROM adjust_busdate)
	ORDER BY end_date, stock_code;
QUIT;
%get_date_windows(raw_table=adjust_busdate, colname=end_date, output_table = adjust_busdate2, start_intval =0, end_intval = &ic_length.);
%cal_intval_return(raw_table=raw_table, group_name=stock_code, price_name=price, date_table=adjust_busdate2, output_table=ot2, is_single = 1);


/** Step2: (必须）取关注的因子 */
/*%LET pool_table = union_pool;*/
%LET pool_table = gxjs_stock_pool;

%LET fname = research_pct;
/*%LET fname = zl_group;*/

DATA subdata;
	SET &pool_table.;
RUN;
PROC SORT DATA = subdata;
	BY end_date;
RUN;

DATA subdata2;
	SET subdata;
	keep stock_code end_date &fname. indus_code indus_name size is_in_pool is_in_zl;
RUN;
PROC SORT DATA = subdata2;
	BY end_date;
RUN;



/** Step3A: 离散因子的分析 */
/** Step3-1: 因子IC **/
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** Step3-2: 分组收益 */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=2);
/** Step3-3: 分组的股票数量 */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=3);
/* Step3-4: 分组的市值 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname., min(size/100000000) AS min_size,
		max(size/100000000) AS max_size, 
		mean(size/100000000) AS mean_size,
		count(1) AS nobs
	FROM subdata2
	GROUP BY end_Date, &fname.;
QUIT;

/*** Step4A：根据离散变量分组 */
%cut_subset(input_table=subdata2, colname=&fname., output_table=subdata2_filter,
	type=2, threshold=1, is_decrease=2, is_cut=1);



/** Step3B: 连续变量型因子的分析 */
/** Step3-1: 因子IC **/
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** Step3-2: 根据连续变量分组 */
%single_factor_score(raw_table=subdata2, identity=stock_code, factor_name=&fname.,
		output_table=r_results, is_increase = 1, group_num = 5);
/** Step3-3: 分组收益 */
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=2);
/** Step3-4: 分组股票数量 */
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=3);
/* Step3-5: 分组的市值 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname._score, min(size/100000000) AS min_size, 
		max(size/100000000) AS max_size, 
		mean(size/100000000) AS mean_size,
		count(1) AS nobs
	FROM r_results
	GROUP BY end_Date, &fname._score;
QUIT;

/*** Step4B(必须!!)：根据连续变量分组 */
/** 选取因子得分排名前面的50% **/
%cut_subset(input_table=subdata2, colname=&fname., output_table=subdata2_filter,
	type=1, threshold=50, is_decrease=1, is_cut=1);


/*** Step5: 加入主观判断条件 **/
/** 因为中性化对股票数量有一定限制，所以要求在每次调仓时单个行业的股票数量至少有6个(含)。*/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT end_date, indus_name, count(1) AS nobs
	FROM subdata2_filter
	GROUP BY end_date, indus_name;
QUIT;
PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.*
	FROM subdata2_filter A LEFT JOIN tmp B
	ON A.end_date = B.end_Date AND A.indus_name = B.indus_name
	WHERE B.nobs >= 6
	ORDER BY A.end_Date, A.indus_name;
QUIT;
DATA subdata2_filter2;
	SET tmp2;
RUN;




/*** Step6: 统计分析 **/
%LET stat_table = subdata2_filter2;

/** 标注是否在富国股票池 */
%mark_in_table(input_table=&stat_table., cmp_table=fg_stock_pool, 
	mark_col=is_in_fg, output_table=&stat_table., is_strict=0);
/**计算流通市值 */
%get_stock_size(stock_table=&stat_table., info_table=hqinfo, share_table=fg_wind_freeshare,output_table=&stat_table., 
	colname=free_value, index = 1);

/* Step5-1: 行业分布 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, indus_name, count(1) AS nobs
	FROM &stat_table.
	GROUP BY indus_name, end_Date;
QUIT;
PROC TRANSPOSE DATA = stat OUT = stat(drop = _NAME_);
	BY indus_name;
	VAR nobs;
	ID end_Date;
RUN;

/** Step5-2: 富国股票池覆盖 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, sum(is_in_fg) AS is_in_fg, sum(1-is_in_fg) AS not_in_fg
	FROM &stat_table.
	GROUP BY end_date;
QUIT;

/** Step5-3：中证股票池覆盖 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, sum(is_in_pool) AS is_in_pool, sum(1-is_in_fg) AS not_in_pool
	FROM &stat_table.
	GROUP BY end_date;
QUIT;

/** Step5-4：专利池覆盖 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, sum(is_in_zl) AS is_in_zl, sum(1-is_in_zl) AS not_in_zl
	FROM &stat_table.
	GROUP BY end_date;
QUIT;

/** Step5-5：行业流通市值 */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, indus_name, sum(free_value)/100000000 AS free_value
	FROM &stat_table.
	GROUP BY indus_name, end_date;
QUIT;
PROC TRANSPOSE DATA = stat OUT = stat(drop = _NAME_);
	BY indus_name;
	VAR free_value;
	ID end_Date;
RUN;
