/*** ������׼�� **/

/*** ���Ӽ��� **/
%LET product_dir = F:\Research\GIT_BACKUP\tech;
%LET utils_dir = F:\Research\GIT_BACKUP\utils\SAS\�޸İ汾; 

%LET input_dir = &product_dir.\input_data; 
%LET output_dir = &product_dir.\output_data;
LIBNAME product "&product_dir.\sasdata";

%INCLUDE "D:\Research\CODE\initial_sas.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\Ȩ��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ϲ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\�¼��о�_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\������Ч��_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\����_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\���Ӽ���_ͨ�ú���.sas";
%INCLUDE "&utils_dir.\��Ч����_ͨ�ú���.sas";

options validvarname=any; /* ֧�����ı����� */


/******************************* ������*******/
%LET env_start_date = 15may2008;
%INCLUDE "&utils_dir.\�¼��о�_�����ļ�.sas";

/*** ������ͨ��ֵ�� */
PROC SQL;
	CREATe TABLE fg_wind_freeshare AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		freeshare, total_share, a_share, liqa_share
	FROM tinysoft.fg_wind_freeshare
	WHERE datepart(end_date) >= "&env_start_date."d
	ORDER BY end_date, stock_code;
QUIT;

/** ��ҵ��Ϣ�� */
PROC SQL;
	CREATe TABLE fg_wind_sector AS
	SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.,
		o_code AS indus_code, o_name AS indus_name
	FROM bk.fg_wind_sector
	WHERE datepart(end_date) >= "&env_start_date."d
	ORDER BY end_date, stock_code;
QUIT;

/** ���ӵ÷ֱ� */
/** ��oracle����������ʱ�� */
/*PROC SQL;*/
/*	CREATE TABLE product.fg_raw_score AS*/
/*	SELECT **/
/*	FROM fgtest.carl_fg_raw_score;*/
/*QUIT;*/



/** ����������Ʊ��(��800�ɷֹɣ������ɶ�) */
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

/** ȡ6��ĩ�����һ�죬��Ϊ�ǹ�Ʊ�� */
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

/** ����������Ʊ��2 */
PROC SQL;
	CREATE TABLE fg_stock_pool2 AS
	(
		SELECT stock_code, datepart(end_date) AS end_date FORMAT yymmdd10.
		FROM tinysoft.fg_eps_info
		WHERE cnum >= 4 AND datepart(end_date) >= "&env_start_date."d AND stock_code NOT IN ("600837", "000166")
	) 
	ORDER BY end_date;
QUIT;

/** ȡ6��ĩ�����һ�죬��Ϊ�ǹ�Ʊ�� */
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
/** ��Ϊ��Ʊ�ص����ڣ��еĲ��ǽ�������Ҫ���е��� */
%adjust_date_modify(busday_table=busday , raw_table=fg_stock_pool2 ,colname=end_date,  
	output_table=fg_stock_pool2, is_forward = 0 );
DATA fg_stock_pool2(drop = adj_end_date end_date_is_busday);
	SET fg_stock_pool2;
	end_date = adj_end_date;
RUN;

/** ������¼����������� */
%read_from_excel(excel_path=&input_dir.\�������ݵ���.xlsx, output_table=gxjs_stock_pool, sheet_name = ���¼�����Ʊ��$);
%read_from_excel(excel_path=&input_dir.\�������ݵ���.xlsx, output_table=product.report_data, sheet_name = �Ʊ�����$);

/** ��Ϊ�����ݿ����ɹ�Ʊ��(������׼��.sas��) */
/*%read_from_excel(excel_path=&input_dir.\�������ݵ���.xlsx, output_table=product.fg_stock_pool, sheet_name = ������Ʊ��$);*/


