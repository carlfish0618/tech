/*** 专利因子测试：(仅限于股票池内，有专利的样本) **/


%LET adjust_start_date = 5jun2012;   
%LET adjust_end_date = 30mar2015;
%LET test_start_date = 29jun2012;   
%LET test_end_date = 30apr2015;


/** 分析2: **/
%LET pool_table_raw = subdata4_filter;
%LET pool_table = pool_zl_factor;

/********************* 以下模块：统一计算月度收益，适合于因子IC计算 *******/
/** Step1: 计算单月或累计收益率，用于之后计算IC或分组收益 ***/

%get_month_date(busday_table=busday, start_date=5may2012, end_date=31jul2015, 
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

/****************************** 模块结束 ******************/

/** Step1: 将股票池扩展为每个月底 */
%get_month_date(busday_table=busday, start_date=&adjust_start_date., end_date=&adjust_end_date., 
	rename=end_date, output_table=month_busdate, type=1);

PROC SQL;
	CREATE TABLE adjust_busdate AS
	SELECT distinct end_date
	FROM &pool_table_raw.
	ORDER bY end_Date;
QUIT;
%adjust_date_to_mapdate(rawdate_table=month_busdate, mapdate_table=adjust_busdate, 
		raw_colname=end_date, map_colname=end_date, output_table=month_busdate,
		is_backward=1, is_included=1);
PROC SQL;
	CREATE TABLE &pool_table. AS
	SELECT A.end_date, B.stock_code, B.research_pct
	FROM month_busdate A LEFT JOIN &pool_table_raw. B
	ON A.map_end_date = B.end_date
	ORDER BY A.end_date, B.stock_code;
QUIT;

/********************************** 开始：以下仅适合于zl_factor ****/
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, 
	coalesce(cur_zl,0) AS cur_zl,
	coalesce(cur_zl_dif3,0) AS cur_zl_dif3,
	coalesce(cur_zl_dif6,0) AS cur_zl_dif6,
	coalesce(cur_zl_dif36,0) AS cur_zl_dif36,
	coalesce(cur_zl_t3,0) AS cur_zl_t3,
	coalesce(cur_zl_t6,0) AS cur_zl_t6
	FROM &pool_table. A LEFT JOIN zl_factor B
	ON A.end_date = B.end_Date AND A.stock_code = B.stock_code
	ORDER BY A.end_Date, A.stock_code;
QUIT;
DATA &pool_table.;
	SET tmp;
RUN;
%cal_dist(input_table=&pool_table., by_var=end_date, cal_var=cur_zl, out_table=stat);
%cal_dist(input_table=&pool_table., by_var=end_date, cal_var=cur_zl_dif6, out_table=stat);

DATA &pool_table.(drop = i);
	SET &pool_table.;
	ARRAY var_list(6) cur_zl cur_zl_dif3 cur_zl_dif6 cur_zl_dif36 cur_zl_t3 cur_zl_t6;
	ARRAY var_list_m(6) cur_zl_m cur_zl_dif3_m cur_zl_dif6_m cur_zl_dif36_m cur_zl_t3_m cur_zl_t6_m;
	DO i = 1 TO 6;
		IF var_list(i) < 0 THEN var_list_m(i) = 1;
		ELSE IF var_list(i) = 0 THEN var_list_m(i) = 2;
		ELSE IF var_list(i) > 0 THEN var_list_m(i) = 3;
	END;
RUN;
%cal_dist(input_table=&pool_table., by_var=end_date, cal_var=cur_zl_m, out_table=stat);


/** 剔除非上市情况 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.list_date, B.delist_date, B.bk
	FROM &pool_table. A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_Date, A.stock_code;
QUIT;
DATA &pool_table.;
	SET tmp;
	IF list_date >= end_date - 90 THEN mark = 1;
	ELSE IF not missing(delist_date) AND delist_date <= end_date THEN mark = 2;
	ELSE mark = 0;
	IF mark = 0;
RUN;

/** Step1: 因子标准化和winsorize */
/* 取自由流通市值 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.fmv_sqr
	FROM &pool_table. A LEFT JOIN product.fg_raw_score B
	ON A.end_date = datepart(B.end_Date) AND A.stock_code = B.stock_code
	ORDER BY A.end_Date, A.stock_code;
QUIT;
DATA &pool_table.;
	SET tmp;
RUN;


DATA &pool_table.2;
	SET &pool_table.;
	IF end_date >= "15jun2012"d;
	keep stock_code end_date fmv_sqr;
	cur_zl_log = log(cur_zl+1);
	cur_zl_t3_log = log(cur_zl_t3+1);
	cur_zl_t6_log = log(cur_zl_t6+1);
/*	KEEP cur_zl_m cur_zl_dif3_m cur_zl_dif6_m cur_zl_dif36_m cur_zl_t3_m cur_zl_t6_m;*/
	KEEP cur_zl_dif3 cur_zl_dif6;
/*	KEEP cur_zl cur_zl_t3 cur_zl_t6;*/
/*	KEEP cur_zl_log cur_zl_t3_log cur_zl_t6_log;*/
	IF cur_zl > 0;
RUN;


/** 要求当月有专利数据 */
%MACRO test_single_factor(factor_name, cut_value);
	/** 仅限于当月有专利数据的样本 */
	DATA &pool_table.3;
		SET &pool_table.2;
		IF end_date >= "15jun2012"d;
		IF  &factor_name.> &cut_value.;
		KEEP end_date stock_code fmv_sqr &factor_name.;
	RUN;

	/**Step1: 标准化 */
	%normalize_single_score(input_table=&pool_table.3, colname=&factor_name., output_table=&pool_table.3, is_replace = 1);
	%winsorize_single_score(input_table=&pool_table.3, colname=&factor_name., output_table=&pool_table.3, upper=3, lower = -3, is_replace = 1);
	
	/** Step2: 因子分布情况 */
/*	%cal_dist(input_table=&pool_table.3, by_var=end_date, cal_var=&factor_name., out_table=stat);*/
/*	PROC SQL;*/
/*		CREATE TABLE stat2 AS*/
/*		SELECT mean(mean) AS mean,*/
/*		mean(std) AS std,*/
/*		mean(p100) AS p100,*/
/*		mean(p90) AS p90,*/
/*		mean(p75) AS p75,*/
/*		mean(p50) AS p50,*/
/*		mean(p25) AS p25,*/
/*		mean(p10) AS p10,*/
/*		mean(p0) AS p0*/
/*		FROM stat;*/
/*	QUIT;*/
/*	%output_to_excel(excel_path=&output_dir.\&factor_name..xls, input_table=stat2, sheet_name = dist);*/

	/** Step3: 因子IC */
/*	%test_factor_ic(pool_table=&pool_table.3, fname=&factor_name.);*/

	/** Step4: 分组测试，计算alpha **/
	/** Step4-1: 基准 */
	/* (1) 回测日期*/
	DATA test_busdate;
		SET busday(keep = date);
		IF "&test_start_date."d <= date <= "&test_end_date."d;
	RUN;

	/* (2) 调仓日期: 每个月月末 */
	/* 月末数据 */
	PROC SQL;
		CREATE TABLE month_busdate AS
		SELECT date AS end_date LABEL "end_date"
		FROM busday
		GROUP BY year(date), month(date)
		HAVING date = max(date);
	QUIT;

	DATA adjust_busdate;
		SET month_busdate;
		IF "&adjust_start_date."d <= end_date <= "&adjust_end_date."d;
	RUN;

	/** 生成等权基准: bm_equal */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT end_date, stock_code, 1 AS weight
		FROM &pool_table.3;
	QUIT;
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	%gen_daily_pool(stock_pool=test_stock_pool, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
	%cal_stock_wt_ret(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
	%cal_portfolio_ret(daily_stock_pool=test_stock_pool, output_daily_summary=bm_equal);

	/** 生成加权基准: bm_weight */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT end_date, stock_code
		FROM &pool_table.3;
	QUIT;
	%get_stock_size(stock_table=test_stock_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
		output_table=test_stock_pool, colname=weight, index = 1);
	%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
	%gen_daily_pool(stock_pool=test_stock_pool, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
	%cal_stock_wt_ret(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
	%cal_portfolio_ret(daily_stock_pool=test_stock_pool, output_daily_summary=bm_weight);

	/** Step4-2: 分组收益 */
	/** 适合于连续变量+单因子 */
/*	%test_factor_group_ret(pool_table=&pool_table.3, fname=&factor_name., ngroup =3, is_cut=1)*/
	%test_factor_higher_group_ret(pool_table=&pool_table.3, fname=&factor_name., nstock=100, bm_table=bm_equal);

%MEND test_single_factor;

%test_single_factor(factor_name=cur_zl_t6, cut_value=0);
%test_single_factor(factor_name=cur_zl_t6_log, cut_value=0);
%test_single_factor(factor_name=cur_zl_t3, cut_value=0);
%test_single_factor(factor_name=cur_zl_t3_log, cut_value=0);
%test_single_factor(factor_name=cur_zl, cut_value=0);
%test_single_factor(factor_name=cur_zl_log, cut_value=0);


%test_single_factor(factor_name=cur_zl_dif3, cut_value=.);
%test_single_factor(factor_name=cur_zl_dif6, cut_value=.);

/** Step4：汇总结果 */
/* Step4-1: 因子ic */
%merge_timeseries(merge_var=s_ic_f1, suffix = ic, output_table=ic_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"), is_hit=0, factor_table=&pool_table.2);
%merge_timeseries(merge_var=s_ic_f1, suffix = ic, output_table=ic_hit_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"), is_hit=1, factor_table=&pool_table.2);

/* Step4-2: 因子覆盖 */
%merge_timeseries(merge_var=pct, suffix = cover, output_table=cover_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"), is_hit=0, factor_table=&pool_table.2);

/* Step4-3: 分三组结果 */
%merge_result(merge_var=nstock, output_table=nstock_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2 );
%merge_result(merge_var=accum_ret, output_table=ret_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2 );
%merge_result(merge_var=sd, output_table=sd_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result(merge_var=ir, output_table=ir_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result(merge_var=hit_ratio, output_table=hit_ratio_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);

/** Step4-4: top100结果*/
/** 等权组合 */
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=accum_ret, output_table=ret_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=sd, output_table=sd_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=ir, output_table=ir_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=hit_ratio, output_table=hit_ratio_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=turnover, output_table=turnover_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=nstock, output_table=nstock_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);

/**等权组合-分年度 */
%merge_result_higher_total(input_pre=100_stat_e, year = 2012,  merge_var=accum_ret, output_table=ret_stat_e2012,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 2013,  merge_var=accum_ret, output_table=ret_stat_e2013,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 2014,  merge_var=accum_ret, output_table=ret_stat_e2014,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_e, year = 2015,  merge_var=accum_ret, output_table=ret_stat_e2015,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);

/** 加权组合 */
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=accum_ret, output_table=ret_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=sd, output_table=sd_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=ir, output_table=ir_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=hit_ratio, output_table=hit_ratio_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=turnover, output_table=turnover_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=nstock, output_table=nstock_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);

/**加权组合-分年度 */
%merge_result_higher_total(input_pre=100_stat_w, year = 2012,  merge_var=accum_ret, output_table=ret_stat_w2012,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 2013,  merge_var=accum_ret, output_table=ret_stat_w2013,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 2014,  merge_var=accum_ret, output_table=ret_stat_w2014,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);
%merge_result_higher_total(input_pre=100_stat_w, year = 2015,  merge_var=accum_ret, output_table=ret_stat_w2015,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME","INDUS_CODE"),factor_table=&pool_table.2);



PROC SQL;
	CREATE TABLE tmp1 AS
	SELECT A.factor, A.accum_ret, B.sd, C.ir, D.hit_ratio,
	E.accum_ret AS y2012,
	F.accum_ret AS y2013,
	G.accum_ret AS y2014,
	H.accum_ret AS y2015,
	I.nstock AS nstock,
	J.turnover AS turnover
	FROM ret_stat_e A 
	JOIN sd_stat_e B
	ON A.factor = B.factor
	JOIN ir_stat_e C
	ON A.factor = C.factor
	JOIN hit_ratio_stat_e D
	ON A.factor = D.factor
	JOIN ret_stat_e2012 E
	ON A.factor = E.factor
	JOIN ret_stat_e2013 F
	ON A.factor = F.factor
	JOIN ret_stat_e2014 G
	ON A.factor = G.factor
	JOIN ret_stat_e2015 H
	ON A.factor = H.factor
	JOIN nstock_stat_e I
	ON A.factor = I.factor
	JOIN turnover_stat_e J
	ON A.factor = J.factor
	ORDER BY A.factor;
QUIT;

PROC SQL;
	CREATE TABLE tmp2 AS
	SELECT A.factor, A.accum_ret, B.sd, C.ir, D.hit_ratio,
	E.accum_ret AS y2012,
	F.accum_ret AS y2013,
	G.accum_ret AS y2014,
	H.accum_ret AS y2015,
	I.nstock AS nstock,
	J.turnover AS turnover
	FROM ret_stat_w A 
	JOIN sd_stat_w B
	ON A.factor = B.factor
	JOIN ir_stat_w C
	ON A.factor = C.factor
	JOIN hit_ratio_stat_w D
	ON A.factor = D.factor
	JOIN ret_stat_w2012 E
	ON A.factor = E.factor
	JOIN ret_stat_w2013 F
	ON A.factor = F.factor
	JOIN ret_stat_w2014 G
	ON A.factor = G.factor
	JOIN ret_stat_w2015 H
	ON A.factor = H.factor
	JOIN nstock_stat_w I
	ON A.factor = I.factor
	JOIN turnover_stat_w J
	ON A.factor = J.factor
	ORDER BY A.factor;
QUIT;

PROC SQL;
	DROP TABLE ret_stat_e, sd_stat_e, ir_stat_e, hit_ratio_stat_e, ret_stat_e2012, ret_stat_e2013, ret_stat_e2014, ret_stat_e2015, nstock_stat_e, turnover_stat_e,
		ret_stat_w, sd_stat_w, ir_stat_w, hit_ratio_stat_w, ret_stat_w2012, ret_stat_w2013, ret_stat_w2014, ret_stat_w2015, nstock_stat_w, turnover_stat_w;
QUIT;
