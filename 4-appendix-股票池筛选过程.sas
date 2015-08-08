/** ������Ʊ�� **/

/** (1) zl_change
(2) gxjs_stock_pool
(3) union_pool 
**/

%LET fname = research_pct;
/******************************* PART I: ר������¼�����Ʊ�ؽ��кϲ�������union_pool ********************/

/** ��gxjs_stock_pool���� */
/**ע��: gxjs_stock_pool�е�year����ʾ��ʼ��Ч��ݡ�
	��zl_change�е�att_year���з����õ����(��gxjs_stock_pool�е�research0��һ�������)��
	����һ�����һ�ꡣ
***/

/** Step1: ��ȥgxjs_stock_pool��zl_change */
%LET pool_table = gxjs_stock_pool;
%INCLUDE "&product_dir.\sascode\3-��Ʊ�ع���.sas";
%LET pool_table = zl_change;
%INCLUDE "&product_dir.\sascode\3-��Ʊ�ع���.sas";


/** Step2: ������ߵ��غ϶� */
PROC SQL;
	CREATE TABLE cmp AS
	SELECT A.year AS year_pool, A.stock_code AS stock_code_pool, 
		B.stock_code AS stock_code_zl,
		B.att_year+1 AS year_zl,
		coalesce(A.stock_code, B.stock_code) AS stock_code,
		coalesce(A.year, B.att_year+1) AS year,
		coalesce(B.n_get,0) AS n_get
	FROM gxjs_stock_pool A
	FULL JOIN zl_change B
	ON A.year = B.att_year+1 AND A.stock_code = B.stock_code
	ORDER BY year, stock_code;
QUIT;
DATA cmp;
	SET cmp;
	IF not missing(stock_code_pool) AND not missing(stock_code_zl) THEN mark = 1; /* �����غ� */
	ELSE IF not missing(stock_code_pool) THEN mark = 2; /* �ڹ�Ʊ���У���δ��ר������ */
	ELSE mark = 3; /* ��ר�����ݣ���δ�ڹ�Ʊ���� */
RUN;

PROC SQL;
	CREATE TABLE stat AS
	SELECT year, 
		sum(mark=1 OR mark=2) AS n_pool,
		sum(mark=1 OR mark=3) AS n_zl,
		sum(mark=1) AS n_both
   FROM cmp
	GROUP BY year;
QUIT;


/***** Step3��ȡ�����Ĳ�������Ϊ��Ʊ��union_pool **/
DATA union_pool(keep = stock_code year n_get is_in_pool is_in_zl);
	SET cmp;
	IF year >= 2012;
	IF mark IN (1,2) THEN is_in_pool = 1;
	ELSE is_in_pool = 0;
	IF mark IN (1,3) THEN is_in_zl = 1;
	ELSE is_in_zl = 0;
RUN;


/***** Step4������union_pool��ȡ�û������� **/
%LET pool_table = union_pool;
%INCLUDE "&product_dir.\sascode\3-��Ʊ�ع���.sas";

DATA union_pool;
	SET union_pool;
	IF n_get > 0 THEN change_rate = research0/n_get;   /* ת���ʵĵ��� */
	ELSE change_rate = 0;
	IF n_get > 0 THEN zl_group = 1;
	ELSE zl_group = 0;
RUN;


/******************************* PART II: ȷ����ͬ��Ʊ�ع��췽�� ********************/

%get_sector_info(stock_table=union_pool, mapping_table=fg_wind_sector, output_stock_table=union_pool);
%get_stock_size(stock_table=union_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
	output_table=union_pool, colname=size, index = 3);

/*%cal_dist(input_table=union_pool, by_var=year, cal_var=change_rate, out_table=stat);*/
/*%cal_dist(input_table=union_pool, by_var=year, cal_var=n_get, out_table=stat);*/
/*%cal_dist(input_table=union_pool, by_var=year, cal_var=research_pct, out_table=stat);*/

/** ����1: union��research_pct����ǰ50% */
/** ����subdata1 **/
DATA subdata;
	SET union_pool;
RUN;
PROC SORT DATA = subdata;
	BY end_date;
RUN;

DATA subdata;
	SET subdata;
	keep stock_code end_date research_pct indus_code indus_name size is_in_pool is_in_zl;
RUN;
PROC SORT DATA = subdata;
	BY end_date;
RUN;

%cut_subset(input_table=subdata, colname=&fname., output_table=subdata1,
	type=1, threshold=50, is_decrease=1, is_cut=1);


/** ����2: ��ר������+��ר��������research_pct����ǰ50% */
/** ����subdata2 **/
DATA subdata;
	SET union_pool;
RUN;
PROC SORT DATA = subdata;
	BY end_date;
RUN;

DATA subdata;
	SET subdata;
	keep stock_code end_date research_pct indus_code indus_name size is_in_pool is_in_zl;
RUN;
PROC SORT DATA = subdata;
	BY end_date;
RUN;

%cut_subset(input_table=subdata, colname=&fname., output_table=subdata2,
	type=1, threshold=50, is_decrease=1, is_cut=1);
DATA tmp;
	SET subdata;
	If is_in_zl = 1;
RUN;
DATA subdata2;
	SET subdata2 tmp;
RUN;
PROC SORT DATA = subdata2 NODUP;
	BY _ALL_;
RUN;

/** ����3: ��ר������+research_pct����һ�������� */
/** ����subdata3 **/

DATA subdata3;
	SET union_pool;
	IF is_in_zl = 1 OR re_mark = 1;
RUN;
PROC SORT DATA = subdata3;
	BY end_date;
RUN;

/** ����4: research_pct����һ�������� */
/** ����subdata4 **/

DATA subdata4;
	SET union_pool;
	IF re_mark = 1;
RUN;
PROC SORT DATA = subdata4;
	BY end_date;
RUN;



/******************************* PART III: ���������ж�********************/
/*** Step5: ���������ж����� **/
/** ��Ϊ���Ի��Թ�Ʊ������һ�����ƣ�����Ҫ����ÿ�ε���ʱ������ҵ�Ĺ�Ʊ����������6��(��)��*/
%MACRO filter_pool(input_table, output_table);
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT end_date, indus_name, count(1) AS nobs
		FROM &input_table.
		GROUP BY end_date, indus_name;
	QUIT;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*
		FROM &input_table. A LEFT JOIN tmp B
		ON A.end_date = B.end_Date AND A.indus_name = B.indus_name
		WHERE B.nobs >= 6
		ORDER BY A.end_Date, A.indus_name;
	QUIT;
	DATA &output_table.;
		SET tmp2;
	RUN;
%MEND filter_pool;
%filter_pool(subdata1, subdata1_filter);
%filter_pool(subdata2, subdata2_filter);
%filter_pool(subdata3, subdata3_filter);
%filter_pool(subdata4, subdata4_filter);

/******************************* PART IV: ͳ�Ʒ���********************/
/*** Step6: ͳ�Ʒ��� **/
%LET stat_table = subdata4_filter;

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


/******************************* PART V: ͳ������-���׻�********************/
%LET stat_table = subdata4_filter;
DATA pool;
	SET &stat_table.;
	KEEP end_date stock_code is_in_zl;
RUN;
/* ��ȡ��ҵ��Ϣ */
PROC SQL;
	CREATe TABLE tmp2 AS
	SELECT A.*,
		B.o_code, B.o_name, B.v_code, B.v_name
	FROM pool A LEFT JOIN bk.fg_wind_sector B
	ON A.end_date = datepart(B.end_Date) AND A.stock_code = B.stock_code
	ORDER BY end_date, stock_code;
QUIT;

/** ��TMT��ҵ���в�֡�*/
DATA pool;
	SET tmp2;
	IF o_name = "TMT" THEN indus_name = v_name;
	ELSE indus_name = o_name;
RUN;

/** ��ע�Ƿ��ڸ�����Ʊ�� */
%mark_in_table(input_table=pool, cmp_table=fg_stock_pool, 
	mark_col=is_in_fg, output_table=pool, is_strict=0);

/** ͳ��1����ͬ����У�������ҵ�Ĺ�Ʊ������ר�����������͸�����Ʊ�� */
%MACRO year_summary(year);
	PROC SQL;
		CREATE TABLE stat AS
		SELECT indus_name, count(1) AS nobs, 
		sum(is_in_zl) AS zl_nobs,
		sum(is_in_fg) AS fg_nobs
		FROM pool
		WHERE year(end_date) = &year.
		GROUP BY indus_name
		ORDER BY indus_name;
	QUIT;
%MEND year_summary;
%year_summary(2015);

/** ͳ��2: 2015-6-30����A�ɹ�ע�ȣ���Ʊ�ع�Ʊ��ע�� */
PROC SQL;
	CREATE TABLE stat AS
	SELECT stock_code, coalesce(cnum,0) AS cnum, year(datepart(end_date)) as year
	FROM tinysoft.fg_eps_info
	WHERE datepart(end_date) IN ("30jun2015"d, "30jun2014"d, "30jun2013"d,
	"30jun2012"d, "30jun2011"d)
	ORDER BY year,stock_code;
QUIT;

%cal_dist(input_table=stat, by_var=year, cal_var=cnum, out_table=stat2);

PROC SQL;
	CREATE TABLE stat AS
	SELECT A.stock_code, coalesce(cnum,0) AS cnum, year
	FROM &stat_table. A LEFT JOIN tinysoft.fg_eps_info B
	ON A.end_date = datepart(B.end_date) AND A.stock_code = B.stock_code
	ORDER BY year,stock_code;
QUIT;
%cal_dist(input_table=stat, by_var=year, cal_var=cnum, out_table=stat2);
