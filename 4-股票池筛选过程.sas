/** ����: union_pool + zl_change + gxsj_stock_pool�е�����֮һ */
/** ����: research_pct��zl_group����Ч�������ֶ� */
/*ע: research_pctΪ����������zl_groupΪ��ɢ���������ߵķ���ģ����΢�в��죬ע�����֡�*/

/** ���չ�Ʊ�ص��߼�Ϊ:
(1) research_pct����ǰ50%
(2) ��ҵ����Ҫ�󣺸���һ����ҵ�Ĺ�Ʊ����>=6
***/
/** ��Ʊ�ع���: union_pool --> subdata --> subdata2 --> subdata2_filter --> subdata2_filter2 */

/** ��Ʊ�صĽ��Ϊ: subdata2_filter2 **/




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



/** Step3A: ��ɢ���ӵķ��� */
/** Step3-1: ����IC **/
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** Step3-2: �������� */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=2);
/** Step3-3: ����Ĺ�Ʊ���� */
%single_score_ret(score_table=subdata2, return_table=ot2, identity=stock_code, score_name=&fname.,
	ret_column =., is_transpose = ., type=3);
/* Step3-4: �������ֵ */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_date, &fname., min(size/100000000) AS min_size,
		max(size/100000000) AS max_size, 
		mean(size/100000000) AS mean_size,
		count(1) AS nobs
	FROM subdata2
	GROUP BY end_Date, &fname.;
QUIT;

/*** Step4A��������ɢ�������� */
%cut_subset(input_table=subdata2, colname=&fname., output_table=subdata2_filter,
	type=2, threshold=1, is_decrease=2, is_cut=1);



/** Step3B: �������������ӵķ��� */
/** Step3-1: ����IC **/
%single_factor_ic(factor_table=subdata2, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
/** Step3-2: ���������������� */
%single_factor_score(raw_table=subdata2, identity=stock_code, factor_name=&fname.,
		output_table=r_results, is_increase = 1, group_num = 5);
/** Step3-3: �������� */
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=2);
/** Step3-4: �����Ʊ���� */
%single_score_ret(score_table=r_results, return_table=ot2, identity=stock_code, score_name=&fname._score,
	ret_column =., is_transpose = ., type=3);
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

/*** Step4B(����!!)������������������ */
/** ѡȡ���ӵ÷�����ǰ���50% **/
%cut_subset(input_table=subdata2, colname=&fname., output_table=subdata2_filter,
	type=1, threshold=50, is_decrease=1, is_cut=1);


/*** Step5: ���������ж����� **/
/** ��Ϊ���Ի��Թ�Ʊ������һ�����ƣ�����Ҫ����ÿ�ε���ʱ������ҵ�Ĺ�Ʊ����������6��(��)��*/
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




/*** Step6: ͳ�Ʒ��� **/
%LET stat_table = subdata2_filter2;

/** ��ע�Ƿ��ڸ�����Ʊ�� */
%mark_in_table(input_table=&stat_table., cmp_table=fg_stock_pool, 
	mark_col=is_in_fg, output_table=&stat_table., is_strict=0);
/**������ͨ��ֵ */
%get_stock_size(stock_table=&stat_table., info_table=hqinfo, share_table=fg_wind_freeshare,output_table=&stat_table., 
	colname=free_value, index = 1);

/* Step5-1: ��ҵ�ֲ� */
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

/** Step5-2: ������Ʊ�ظ��� */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, sum(is_in_fg) AS is_in_fg, sum(1-is_in_fg) AS not_in_fg
	FROM &stat_table.
	GROUP BY end_date;
QUIT;

/** Step5-3����֤��Ʊ�ظ��� */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, sum(is_in_pool) AS is_in_pool, sum(1-is_in_fg) AS not_in_pool
	FROM &stat_table.
	GROUP BY end_date;
QUIT;

/** Step5-4��ר���ظ��� */
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, sum(is_in_zl) AS is_in_zl, sum(1-is_in_zl) AS not_in_zl
	FROM &stat_table.
	GROUP BY end_date;
QUIT;

/** Step5-5����ҵ��ͨ��ֵ */
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
