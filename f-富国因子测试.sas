/** �������Ӳ��� */
%LET adjust_start_date = 5jun2012;   
%LET adjust_end_date = 30jun2015;
%LET test_start_date = 29jun2012;   
%LET test_end_date = 24jul2015;



/*%test_multiple_factor_ic(input_table=product.stock_pool_znw2,  */
/*	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));*/
%test_multiple_factor_ic(input_table=product.stock_pool_znw,  
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME", "CABSEPS_GG"));

/** ����IC��� */
%merge_timeseries(merge_var=s_ic_f1, suffix = ic, output_table=ic_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"), is_hit=0);
%merge_timeseries(merge_var=s_ic_f1, suffix = ic, output_table=ic_hit_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"), is_hit=1);
%merge_timeseries(merge_var=pct, suffix = cover, output_table=cover_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"), is_hit=0);



/** Step2: �����鹹���Ȩ��ϣ�����alpha **/
/***  ���� */
/* (1) �ز�����*/
DATA test_busdate;
	SET busday(keep = date);
	IF "&test_start_date."d <= date <= "&test_end_date."d;
RUN;

/* (2) ��������: ÿ������ĩ */
/* ��ĩ���� */
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

/** ���ɵ�Ȩ��׼: bm_equal */
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT end_date, stock_code, 1 AS weight
	FROM product.stock_pool_z;
QUIT;
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
%gen_daily_pool(stock_pool=test_stock_pool, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
%cal_stock_wt_ret(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
%cal_portfolio_ret(daily_stock_pool=test_stock_pool, output_daily_summary=bm_equal);

/** ���ɼ�Ȩ��׼: bm_weight */
PROC SQL;
	CREATE TABLE test_stock_pool AS
	SELECT end_date, stock_code
	FROM product.stock_pool_z;
QUIT;
%get_stock_size(stock_table=test_stock_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
	output_table=test_stock_pool, colname=weight, index = 1);
%neutralize_weight(stock_pool=test_stock_pool, output_stock_pool=test_stock_pool);
%gen_daily_pool(stock_pool=test_stock_pool, test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
%cal_stock_wt_ret(daily_stock_pool=test_stock_pool, adjust_date_table=adjust_busdate, output_stock_pool=test_stock_pool);
%cal_portfolio_ret(daily_stock_pool=test_stock_pool, output_daily_summary=bm_weight);


/** ������Աȣ�ȡtop100�Ա� **/
%test_multiple_factor_group_ret(input_table=product.stock_pool_znw,  
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));


/** ���ܷ�����ԱȵĽ�� */
%merge_result(merge_var=accum_ret, output_table=ret_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result(merge_var=sd, output_table=sd_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result(merge_var=ir, output_table=ir_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result(merge_var=hit_ratio, output_table=hit_ratio_stat,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
	

/** ����ȡǰtop100�Ľ�� */
/** ��Ȩ��� */
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=accum_ret, output_table=ret_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=sd, output_table=sd_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=ir, output_table=ir_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=hit_ratio, output_table=hit_ratio_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=turnover, output_table=turnover_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 0,  merge_var=nstock, output_table=nstock_stat_e,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));

/**��Ȩ���-����� */
%merge_result_higher_total(input_pre=100_stat_e, year = 2012,  merge_var=accum_ret, output_table=ret_stat_e2012,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 2013,  merge_var=accum_ret, output_table=ret_stat_e2013,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 2014,  merge_var=accum_ret, output_table=ret_stat_e2014,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_e, year = 2015,  merge_var=accum_ret, output_table=ret_stat_e2015,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));

/** ��Ȩ��� */
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=accum_ret, output_table=ret_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=sd, output_table=sd_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=ir, output_table=ir_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=hit_ratio, output_table=hit_ratio_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=turnover, output_table=turnover_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 0,  merge_var=nstock, output_table=nstock_stat_w,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));

/**��Ȩ���-����� */
%merge_result_higher_total(input_pre=100_stat_w, year = 2012,  merge_var=accum_ret, output_table=ret_stat_w2012,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 2013,  merge_var=accum_ret, output_table=ret_stat_w2013,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 2014,  merge_var=accum_ret, output_table=ret_stat_w2014,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));
%merge_result_higher_total(input_pre=100_stat_w, year = 2015,  merge_var=accum_ret, output_table=ret_stat_w2015,
	exclude_list=("FMV_SQR", "O_CODE", "O_NAME", "V_CODE", "V_NAME", "INDUS_NAME"));

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

