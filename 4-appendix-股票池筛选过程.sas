/** 构建股票池 **/

/** (1) zl_change
(2) gxjs_stock_pool
(3) union_pool 
**/

%LET fname = research_pct;
/******************************* PART I: 专利与高新技术股票池进行合并，生成union_pool ********************/

/** 与gxjs_stock_pool连接 */
/**注意: gxjs_stock_pool中的year，表示开始生效年份。
	而zl_change中的att_year是研发费用的年份(与gxjs_stock_pool中的research0是一样的年份)。
	二者一般相差一年。
***/

/** Step1: 过去gxjs_stock_pool和zl_change */
%LET pool_table = gxjs_stock_pool;
%INCLUDE "&product_dir.\sascode\3-股票池过滤.sas";
%LET pool_table = zl_change;
%INCLUDE "&product_dir.\sascode\3-股票池过滤.sas";


/** Step2: 计算二者的重合度 */
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
	IF not missing(stock_code_pool) AND not missing(stock_code_zl) THEN mark = 1; /* 二者重合 */
	ELSE IF not missing(stock_code_pool) THEN mark = 2; /* 在股票池中，但未有专利数据 */
	ELSE mark = 3; /* 有专利数据，但未在股票池中 */
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


/***** Step3：取两个的并集，作为股票池union_pool **/
DATA union_pool(keep = stock_code year n_get is_in_pool is_in_zl);
	SET cmp;
	IF year >= 2012;
	IF mark IN (1,2) THEN is_in_pool = 1;
	ELSE is_in_pool = 0;
	IF mark IN (1,3) THEN is_in_zl = 1;
	ELSE is_in_zl = 0;
RUN;


/***** Step4：过滤union_pool，取得基础数据 **/
%LET pool_table = union_pool;
%INCLUDE "&product_dir.\sascode\3-股票池过滤.sas";

DATA union_pool;
	SET union_pool;
	IF n_get > 0 THEN change_rate = research0/n_get;   /* 转换率的倒数 */
	ELSE change_rate = 0;
	IF n_get > 0 THEN zl_group = 1;
	ELSE zl_group = 0;
RUN;


/******************************* PART II: 确定不同股票池构造方法 ********************/

%get_sector_info(stock_table=union_pool, mapping_table=fg_wind_sector, output_stock_table=union_pool);
%get_stock_size(stock_table=union_pool, info_table=hqinfo, share_table=fg_wind_freeshare,
	output_table=union_pool, colname=size, index = 3);

/*%cal_dist(input_table=union_pool, by_var=year, cal_var=change_rate, out_table=stat);*/
/*%cal_dist(input_table=union_pool, by_var=year, cal_var=n_get, out_table=stat);*/
/*%cal_dist(input_table=union_pool, by_var=year, cal_var=research_pct, out_table=stat);*/

/** 方案1: union中research_pct排名前50% */
/** 生成subdata1 **/
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


/** 方案2: 有专利数据+无专利数据中research_pct排名前50% */
/** 生成subdata2 **/
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

/** 方案3: 有专利数据+research_pct满足一定条件的 */
/** 生成subdata3 **/

DATA subdata3;
	SET union_pool;
	IF is_in_zl = 1 OR re_mark = 1;
RUN;
PROC SORT DATA = subdata3;
	BY end_date;
RUN;

/** 方案4: research_pct满足一定条件的 */
/** 生成subdata4 **/

DATA subdata4;
	SET union_pool;
	IF re_mark = 1;
RUN;
PROC SORT DATA = subdata4;
	BY end_date;
RUN;



/******************************* PART III: 加入主观判断********************/
/*** Step5: 加入主观判断条件 **/
/** 因为中性化对股票数量有一定限制，所以要求在每次调仓时单个行业的股票数量至少有6个(含)。*/
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

/******************************* PART IV: 统计分析********************/
/*** Step6: 统计分析 **/
%LET stat_table = subdata4_filter;

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


/******************************* PART V: 统计数据-给幼华********************/
%LET stat_table = subdata4_filter;
DATA pool;
	SET &stat_table.;
	KEEP end_date stock_code is_in_zl;
RUN;
/* 提取行业信息 */
PROC SQL;
	CREATe TABLE tmp2 AS
	SELECT A.*,
		B.o_code, B.o_name, B.v_code, B.v_name
	FROM pool A LEFT JOIN bk.fg_wind_sector B
	ON A.end_date = datepart(B.end_Date) AND A.stock_code = B.stock_code
	ORDER BY end_date, stock_code;
QUIT;

/** 将TMT行业进行拆分。*/
DATA pool;
	SET tmp2;
	IF o_name = "TMT" THEN indus_name = v_name;
	ELSE indus_name = o_name;
RUN;

/** 标注是否在富国股票池 */
%mark_in_table(input_table=pool, cmp_table=fg_stock_pool, 
	mark_col=is_in_fg, output_table=pool, is_strict=0);

/** 统计1：不同年份中，各个行业的股票数量，专利覆盖数量和富国股票池 */
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

/** 统计2: 2015-6-30所有A股关注度，股票池股票关注度 */
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
