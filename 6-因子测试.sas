/** 6- �������Ӳ��� */
/**��ר�����ӣ�
(1) ֱ�Ӹ��ݵ��¾���ֵ��������/�ޣ�����֮��Ĳ���س�̫��
(2) ֱ�Ӹ��ݵ��¾���ֵ������IC��IC��������������ʱ�����Ƶ����Բ���
(3) ���ݵ��¾���ֵ��ȹ�ȥN���µı仯 **/


/** Step1: ���㵥�»��ۼ������ʣ�����֮�����IC��������� ***/
/** ��ĩ����*/
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



/** Step2: (���룩ȡ��ע������ */

%LET pool_table = stock_pool_znw2;
%LET fname = crateps_gg;
/** 1- ԭʼ����ֵ */
DATA test_pool(keep = end_date stock_code &fname.);
	SET &pool_table.;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, sum(not missing(&fname.))/count(1) AS pct
	FROM test_pool
	GROUP BY end_date;
QUIT;
%output_to_excel(excel_path, input_table, sheet_name = data)
%get_sector_info(stock_table=test_pool, mapping_table=fg_wind_sector, output_stock_table=test_pool);


/** 2- ԭʼ����ֵ�ӹ�Ϊ��ɢ���� */
DATA test_pool;
	SET &pool_table.;
	IF &fname. > 0 THEN &fname._m = 2;
	ELSE IF not missing(&fname.) THEN &fname._m = 1;
	ELSE &fname._m = 0;
RUN;
%get_sector_info(stock_table=test_pool, mapping_table=fg_wind_sector, output_stock_table=test_pool);


PROC SORT DATA = test_pool;
	BY end_date;
RUN;
%LET fname = &fname._m;

%cal_dist(input_table=test_pool, by_var=end_date, cal_var=dif, out_table=stat);

/** 3- �˹������ӹ� */
DATA test_pool;
	SET zl_factor2;
/*	type = (nobs_prev2>0)*4 + (nobs_prev1>0)*2 + (nobs>0);*/
/*	IF type = 0 THEN type1 = 0;*/
/*	ELSE IF type = 7 THEN type1 = 1;*/
/*	ELSE type1 = 2;*/
/*	IF nobs>0 and nobs_prev1>0 and nobs_prev2 > 0 and nobs_prev3 >0 and nobs_prev4>0 and nobs_prev5>0 THEN type1 = 1; */
/*	ELSE IF nobs+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>0 THEN type1 = 2;*/
/*	ELSE type1 = 3; */
	/** ��ȥ���� */
/*	IF nobs+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>0 THEN type1 = 1; */
/*	ELSE type1 = 2; */
	/** ��ȥ3���� */
/*	IF nobs+nobs_prev1+nobs_prev2>0 THEN type1 = 1; */
/*	ELSE type1 = 2; */

/*	IF nobs+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>0 THEN DO;*/
/*		IF nobs > 0 AND nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>=0 THEN type1 = 1; */
/*		ELSE IF nobs = 0 AND nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>0 THEN type1 = 3;*/
/*	END;*/
/*	ELSE type1 = 4; */
/*	IF nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5 > 0 THEN */
/*		type1 = (nobs_prev0+nobs_prev1+nobs_prev2)/3-(nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5)/6; */
/*	ELSE type1 = .;*/
/** �������� */
/*	IF nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5 > 0 THEN */
/*		type1 = 1;*/
/*	ELSE type1 = 0;*/
	
	IF nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5+nobs_prev6+nobs_prev7+nobs_prev8+nobs_prev9+nobs_prev10+nobs_prev11 > 0 THEN
		type1 = 1;
	ELSE type1 = 0;

/*	IF nobs_prev0 > 0 THEN*/
/*		type1 = 1;*/
/*	ELSE type1 = 0;*/

/*	IF nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5 > 0 THEN DO;*/
/*		IF nobs_prev6+nobs_prev7+nobs_prev8+nobs_prev9+nobs_prev10+nobs_prev11 > 0 THEN type1 = 1;*/
/*		ELSE type1 = 2;*/
/*	END;*/
/*	ELSE DO;*/
/*		IF nobs_prev6+nobs_prev7+nobs_prev8+nobs_prev9+nobs_prev10+nobs_prev11 = 0 THEN type1 = 3;*/
/*		ELSE type1 = 4;*/
/*	END;*/

/*	IF nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5 > 0 THEN DO;*/
/*		IF nobs_prev0+nobs_prev1+nobs_prev2 > 0 AND nobs_prev3+nobs_prev4+nobs_prev5=0 THEN type1 = 1;*/
/*		ELSE IF nobs_prev0+nobs_prev1+nobs_prev2 = 0 AND nobs_prev3+nobs_prev4+nobs_prev5>0 THEN typ1 = 2;*/
/*		ELSE type1 = 3;*/
/*	END;*/
/*	ELSE DO;*/
/*		type1 = 0;*/
/*	END;*/


/*	IF nobs+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5>0 THEN DO;*/
/*		type1 = (nobs>0)+(nobs_prev1>0)+(nobs_prev2>0)*/
/*			+ (nobs_prev3>0) + (nobs_prev4>0) + (nobs_prev5>0);*/
/*	END;*/
/*	ELSE type1 = 0;*/

RUN;
%get_sector_info(stock_table=test_pool, mapping_table=fg_wind_sector, output_stock_table=test_pool);
/*PROC SQL;*/
/*	CREATE TABLE stat AS*/
/*	SELECT end_date, type,count(1) AS nobs*/
/*	FROM test_pool*/
/*	GROUP BY end_date, type;*/
/*QUIT;*/

/** ���������� */
DATA test_pool;
	SET test_pool;
	IF not missing(type1);
RUN;
%cal_dist(input_table=test_pool, by_var=end_date, cal_var=type1, out_table=stat);




%LET fname = type1;
/** Step3A: ��ɢ���ӵķ��� */
/** Step3-1: ����IC **/
%single_factor_ic(factor_table=test_pool, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** Step3-2: ��������(�����·�)*/
%single_score_ret(score_table=test_pool, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=2);
/** Step3-3: ����Ĺ�Ʊ���� */
%single_score_ret(score_table=test_pool, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=3);

/* (δ��һ����) */
%single_score_ret(score_table=test_pool, return_table=ot2, identity=stock_code, score_name=&fname., ret_column =ret_f1, is_transpose = 1, type=3);
%single_score_ret(score_table=test_pool, return_table=ot2, identity=stock_code, score_name=&fname., ret_column =ret_f1, is_transpose = 1, type=2);

/* Step3-4: �������ֵ */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname., min(size/100000000) AS min_size,
		max(size/100000000) AS max_size, 
		mean(size/100000000) AS mean_size,
		count(1) AS nobs
	FROM test_pool
	GROUP BY end_Date, &fname.;
QUIT;


/*** Step4A��������ɢ�������� */
%cut_subset(input_table=subdata2, colname=&fname., output_table=subdata2_filter,
	type=2, threshold=1, is_decrease=2, is_cut=1);



/** Step3B: �������������ӵķ��� */
/** Step3-1: ����IC **/
%single_factor_ic(factor_table=test_pool, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** Step3-2: ���������������� */
%single_factor_score(raw_table=test_pool, identity=stock_code, factor_name=&fname.,
		output_table=r_results, is_increase = 1, group_num = 3);
/** Step3-3: �������� */
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=2);
/** Step3-4: �����Ʊ���� */
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=3);

/* (δ��һ����) */
%single_score_ret(score_table=test_pool, return_table=ot2, identity=stock_code, score_name=&fname._score, ret_column =ret_f1, is_transpose = 1, type=3);
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score, ret_column =ret_f1, is_transpose = 1, type=2);

/* Step3-5: �������ֵ */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname._score, min(size/100000000) AS min_size, 
		max(size/100000000) AS max_size, 
		mean(size/100000000) AS mean_size,
		count(1) AS nobs
	FROM r_results
	GROUP BY end_Date, &fname._score;
QUIT;
