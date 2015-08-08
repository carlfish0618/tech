%LET pool_table = union_pool;
%LET fname = zl_group;

DATA union_pool;
	SET union_pool;
	IF n_get > 0 THEN change_rate = research0/n_get;   /* 转换率的倒数 */
	ELSE change_rate = 0;
	IF n_get > 0 THEN zl_group = 1;
	ELSE zl_group = 0;
RUN;

%cal_dist(input_table=&pool_table., by_var=year, cal_var=change_rate, out_table=stat);



/** Step3: 因子研究 */
%LET adjust_start_date = 5may2012;   
%LET adjust_end_date = 30jun2015;
DATA subdata;
	SET &pool_table.;
RUN;
PROC SORT DATA = subdata;
	BY end_date;
RUN;

/*** 因子标准化等 */

DATA subdata2;
	SET subdata;
	keep stock_code end_date &fname. indus_code indus_name;
RUN;
PROC SORT DATA = subdata2;
	BY end_date;
RUN;
/** 取自由流通市值*/
%get_stock_size(stock_table=subdata2, info_table=hqinfo, share_table=fg_wind_freeshare,output_table=subdata3, colname=fmv_sqr, index = 1);
%normalize_multi_score(input_table=subdata3, output_table=subdata3, exclude_list=("INDUS_CODE", "INDUS_NAME"));
%neutralize_multi_score(input_table=subdata3, output_table=subdata4, group_name = indus_code,  exclude_list=("INDUS_CODE", "INDUS_NAME"));


/** 统计research_pct的分布, 按照中位数进行分组 */
PROC UNIVARIATE DATA = subdata NOPRINT;
	BY end_date;
	VAR research_pct;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.p50
	FROM subdata A LEFT JOIN stat B
	ON A.end_date = B.end_date
	ORDER BY A.end_date;
QUIT;
DATA subdata(drop = p50);
	SET tmp;
	IF research_pct > p50 THEN re_mark3 = 1;
	ELSE IF not missing(research_pct) THEN re_mark3 = 0;
	ELSE re_mark3 = -1;
RUN;

/** 按照revenue和assets的增长情况分组 */
DATA subdata;
	SET subdata;
	IF assets_pct >0 THEN as_mark2 = 1;
	ELSE IF not missing(assets_pct) THEN as_mark2 = 0;
	ELSE as_mark2 = -1;
	IF revenue_pct >0 THEN as_mark3 = 1;
	ELSE IF not missing(revenue_pct) THEN as_mark3 = 0;
	ELSE as_mark3 = -1;
RUN;



/** 统计各类别的样本数 */
%LET stat_var = as_mark2;
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &stat_var., count(1) AS nobs
	FROM subdata
	WHERE  &stat_var. ~= -1
	GROUP BY end_Date,  &stat_var.;
QUIT;
PROC TRANSPOSE DATA = stat prefix = g OUT = stat;
	BY end_date;
	ID  &stat_var.;
	VAR nobs;
RUN;


/** 月末调整*/
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


/** 离散变量分析 */

%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** 收益 */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=2);
/** 股票数量 */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=3);



/* Step3-1：单因子IC分析 ***********/


%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
%single_factor_score(raw_table=subdata2, identity=stock_code, factor_name=&fname.,
		output_table=r_results, is_increase = 1, group_num = 5);
DATA subdata5;
	SET r_results;
	IF not missing(&fname._score);
RUN;
%single_score_ret(score_table=subdata5, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=2);

/** Step3-1-1: 分成N组后，每组因子的因子值范围以及市值范围 */
%get_stock_size(stock_table=subdata5, info_table=hqinfo, share_table=fg_wind_freeshare,output_table=subdata6, colname=size, index = 3);
PROC SORT DATA = subdata6;
	BY descending size;
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname._score, min(size/100000000) AS min_size, max(size/100000000) AS max_size, mean(size/100000000) AS mean_size,
		min(&fname.) AS min_f, max(&fname.) AS max_f, mean(&fname.) AS mean_f,
		count(1) AS nobs
	FROM subdata6
	GROUP BY end_Date, &fname._score;
QUIT;



/** Step3-2: 用mark分组比较 */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = 1, type=2);

/** Step3-2-1: 分成N组后，每组因子的因子值范围以及市值范围 */
%get_stock_size(stock_table=subdata, info_table=hqinfo, share_table=fg_wind_freeshare,output_table=subdata2, colname=size, index = 3);
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname., min(size/100000000) AS min_size, max(size/100000000) AS max_size, mean(size/100000000) AS mean_size,
		count(1) AS nobs
	FROM subdata2
	GROUP BY end_Date, &fname.;
QUIT;


/**** Step4: 更换样本空间，认为是所有的A股 ***/
/** 所有有研发费用的股票中，是否研发费用占比可作为筛选条件。 */


