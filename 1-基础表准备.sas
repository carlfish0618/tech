/*** 基础表准备 **/

/*** 因子检验 **/
%LET product_dir = F:\Research\GIT_BACKUP\tech;
%LET utils_dir = F:\Research\GIT_BACKUP\utils\SAS\修改版本; 

%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "D:\Research\CODE\initial_sas.sas";
%INCLUDE "&utils_dir.\日期_通用函数.sas";
%INCLUDE "&utils_dir.\交易_通用函数.sas";
%INCLUDE "&utils_dir.\权重_通用函数.sas";
%INCLUDE "&utils_dir.\组合构建_通用函数.sas";
%INCLUDE "&utils_dir.\其他_通用函数.sas";
%INCLUDE "&utils_dir.\事件研究_通用函数.sas";
%INCLUDE "&utils_dir.\因子有效性_通用函数.sas";
%INCLUDE "&utils_dir.\计量_通用函数.sas";
%INCLUDE "&utils_dir.\因子计算_通用函数.sas";
%INCLUDE "&utils_dir.\绩效评估_通用函数.sas";

options validvarname=any; /* 支持中文变量名 */


/******************************* 基础表*******/
%LET env_start_date = 15may2008;
%INCLUDE "&utils_dir.\事件研究_配置文件.sas";

/*** 自由流通市值表 */
PROC SQL;
	CREATe TABLE fg_wind_freeshare AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		freeshare, total_share, a_share, liqa_share
	FROM tinysoft.fg_wind_freeshare
	WHERE datepart(end_date) >= "&env_start_date."d
	ORDER BY end_date, stock_code;
QUIT;

/** 行业信息表 */
PROC SQL;
	CREATe TABLE fg_wind_sector AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		o_code AS indus_code, o_name AS indus_name
	FROM bk.fg_wind_sector
	WHERE datepart(end_date) >= "&env_start_date."d
	ORDER BY end_date, stock_code;
QUIT;

/** 因子得分表 */
/** 在oracle中生成了临时表 */
/*PROC SQL;*/
/*	CREATE TABLE product.fg_raw_score AS*/
/*	SELECT **/
/*	FROM fgtest.carl_fg_raw_score;*/
/*QUIT;*/



/** 构建富国股票池(含800成分股，不含股东) */
PROC SQL;
	CREATE TABLE fg_stock_pool AS
	(
		SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.
		FROM tinysoft.fg_eps_info
		WHERE cnum >= 4 AND datepart(end_date) >= "&env_start_date."d AND stock_code NOT IN ("600837", "000166")
	) 
	union
	(
		SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.
		FROM tinysoft.index_info
		WHERE index_code IN ("000300", "000905")  AND datepart(end_date) >= "&env_start_date."d AND stock_code NOT IN ("600837", "000166")
	) 
	ORDER BY end_date;
QUIT;

/** 取6月末的最后一天，认为是股票池 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT *, year(end_date) AS year
	FROM fg_stock_pool
	WHERE month(end_date) = 6 
	GROUP BY year(end_date), month(end_date)
	HAVING end_date = max(end_date)
	ORDER BY end_date, stock_code;
QUIT;
DATA fg_stock_pool;
	SET tmp;
RUN;

/** 构建富国股票池2 */
PROC SQL;
	CREATE TABLE fg_stock_pool2 AS
	(
		SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.
		FROM tinysoft.fg_eps_info
		WHERE cnum >= 4 AND datepart(end_date) >= "&env_start_date."d AND stock_code NOT IN ("600837", "000166")
	) 
	ORDER BY end_date;
QUIT;

/** 取6月末的最后一天，认为是股票池 */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT *, year(end_date) AS year
	FROM fg_stock_pool2
	WHERE month(end_date) = 6 
	GROUP BY year(end_date), month(end_date)
	HAVING end_date = max(end_date)
	ORDER BY end_date, stock_code;
QUIT;
DATA fg_stock_pool2;
	SET tmp;
RUN;
/** 因为股票池的日期，有的不是交易日需要进行调整 */
%adjust_date_modify(busday_table=busday , raw_table=fg_stock_pool2 ,colname=end_date,  
	output_table=fg_stock_pool2, is_forward = 0 );
DATA fg_stock_pool2(drop = adj_end_date end_date_is_busday);
	SET fg_stock_pool2;
	end_date = adj_end_date;
RUN;

/** 读入高新技术基础数据 */
%read_from_excel(excel_path=&input_dir.\基础数据导入.xlsx, output_table=gxjs_stock_pool, sheet_name = 高新技术股票池$);
%read_from_excel(excel_path=&input_dir.\基础数据导入.xlsx, output_table=product.report_data, sheet_name = 财报数据$);

/** 改为从数据库生成股票池(基础表准备.sas中) */
/*%read_from_excel(excel_path=&input_dir.\基础数据导入.xlsx, output_table=product.fg_stock_pool, sheet_name = 富国股票池$);*/


