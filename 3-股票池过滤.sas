/*** ���ֹ�Ʊ�ع��˺�ͳ�� ***/
/** pool_table ����������ѡ��:
(0) zl_change(2-ר��������.sas��������)
(1) gxjs_stock_pool (1-������׼��.sas�Ľ��)
(2) union_pool(0��1�ϲ��Ľ��)

������Ʊ�صĸ�ʽ������: stock_code, year(year=2014��ʾ2014/6/30�յõ��ģ�������2014/7/1-2015/6/30�Ĺ�Ʊ)
***/

/** ��Ʊ�ع��˵�ɸѡ��������:
(1) ����ǰһ��12��31��֮ǰӦ�����С�����Ϊ��6�µ��֣�������ʱ�䳬������
(2) research_pctȱʧ���ߵ���0�Ĺ�Ʊ
**/

/** ���Ĳ���:
(1) ����ÿ���Ʊ�أ��޳��������������ϵĹ�Ʊ
(2) ���ӲƱ�����
(3) �������ֶ�ָ��: research_pct��
(4) �޳�research_pct�����������Ĺ�Ʊ
(5) һЩ���ݵ�ͳ��

**/

/** ������������pool_table: �ⲿ���� */
/*%LET pool_table = gxjs_stock_pool;*/
/*%LET pool_table = union_pool;*/
/*%LET pool_table = zl_change;*/

/** Step1: ����ÿ���Ʊ�� */

/* �޳�δ���е���ҵ */
/* ����ǰһ��12��31��֮ǰӦ�����С�����Ϊ��6�µ��֣�������ʱ�䳬������ */
/** һ���޳�50������ */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.list_date, B.delist_date, B.bk
	FROM &pool_table. A LEFT JOIN stock_info_table B
	ON A.stock_code = B.stock_code
	ORDER BY A.year, A.stock_code;
QUIT;
DATA &pool_table.;
	SET tmp;
	IF list_date >= mdy(12,31,year-1) THEN mark = 1;
	ELSE IF not missing(delist_date) AND delist_date <= mdy(12,31,year-1) THEN mark = 2;
	ELSE mark = 0;
RUN;
DATA &pool_table.(drop = mark list_date delist_date);
	SET &pool_table.;
	IF mark = 0;
	end_date = mdy(6,30,year);
	FORMAT end_date yymmdd10.;
RUN;
/** ����Ϊ6�����һ�������� */
%adjust_date_modify(busday_table=busday , raw_table=&pool_table. ,colname=end_date,  output_table=&pool_table., is_forward = 0 );
DATA &pool_table.(drop = adj_end_date end_date_is_busday);
	SET &pool_table.;
	end_date = adj_end_Date;
RUN;
PROC SORT DATA = &pool_table. NODUPKEY;
	BY _ALL_;
RUN;


/* Step2:���ӲƱ����� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.*
	FROM &pool_table. A LEFT JOIN product.report_data B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
%MACRO add_year_data();
	%DO i = 2012 %TO 2015;
		DATA tmp;
			SET tmp;
			IF year = &i. THEN DO;
				research0 = research%sysevalf(&i.-1);
				research1 = research%sysevalf(&i.-2);
				research2 = research%sysevalf(&i.-3);
				assets0 = assets%sysevalf(&i.-1);
				assets1 = assets%sysevalf(&i.-2);
				assets2 = assets%sysevalf(&i.-3);
				revenue0 = revenue%sysevalf(&i.-1);
				revenue1 = revenue%sysevalf(&i.-2);
				revenue2 = revenue%sysevalf(&i.-3);
			END;
		RUN;
	%END;
%MEND add_year_data;
%add_year_data();
DATA &pool_table.(drop = assets2009--research2014);
	SET tmp;
RUN;


/** Step3: �������ֶ�ָ�� */
DATA &pool_table.;
	SET &pool_table.;
	IF revenue0+revenue1+revenue2>0 THEN
		research_pct = (research0+research1+research2)/(revenue0+revenue1+revenue2);
	ELSE research_pct = .;
	IF assets1>0 AND assets2 > 0 THEN 
		assets_pct = 0.5*(assets0/assets1+assets1/assets2)-1;
	ELSE assets_pct = .;
	IF revenue1 >0 AND revenue2 > 0 THEN 
		revenue_pct = 0.5*(revenue0/revenue1+revenue1/revenue2)-1;
	ELSE revenue_pct = .;
	IF not missing(revenue0) AND revenue0 > 0 THEN DO;
		IF revenue0 < 50000000 AND research_pct >= 0.06 THEN re_mark = 1;
		ELSE IF revenue0 >=50000000 AND research0 < 200000000 AND research_pct >= 0.04 THEN re_mark =1;
		ELSE IF research0 >= 200000000 AND research_pct >= 0.03 THEN re_mark = 1;
		ELSE re_mark = 0;
	END;
	ELSE re_mark = -1;
	IF assets_pct >0 AND revenue_pct >0 THEN as_mark = 1;
	ELSE IF not missing(assets_pct) AND not missing(revenue_pct) THEN as_mark = 0;
	ELSE as_mark = -1;
RUN;

/*DATA tt;*/
/*	SET &pool_table.;*/
/*	IF research_pct = 0 OR missing(research_pct);*/
/*RUN;*/

/** ��3���쳣ֵ���췽ҩҵ���С���ʤ����ȱ��2010����ʲ���ծ������ */
/*DATA tt;*/
/*	SET &pool_table.;*/
/*	IF as_mark = -1 OR re_mark = -1;*/
/*RUN;*/


/*** Step4: �޳�����1��research = 0 ����ȱ��reserach_pct������(20ֻ��Ʊ) */
DATA &pool_table.;
	SET &pool_table.;
	IF research_pct <= 0 OR missing(research_pct) THEN delete;
RUN;


/*** Step5: ͳ�ƽ�� ***/
/** !!ͳ��(���) **/

/*PROC SQL;*/
/*	CREATE TABLE stat2 AS*/
/*	SELECT year, count(1) AS nobs, sum(is_in) AS in_nobs, */
/*		sum(1-is_in) AS not_in_nobs, */
/*		sum(is_in*(bk="����")) AS zb_in,*/
/*		sum(is_in*(bk="��С��ҵ��")) AS zxb_in,*/
/*		sum(is_in*(bk="��ҵ��")) AS cyb_in,*/
/*		sum((bk="����")) AS zb,*/
/*		sum((bk="��С��ҵ��")) AS zxb,*/
/*		sum((bk="��ҵ��")) AS cyb*/
/*	FROM &pool_table.*/
/*	GROUP BY year;*/
/*QUIT;*/

/** ��ҵ�ֲ�*/
%get_sector_info(stock_table=&pool_table., mapping_table=fg_wind_sector, output_stock_table=&pool_table.);
PROC SQL;
	CREATE TABLE stat AS
	SELECT end_Date, indus_name, count(1) AS nobs
	FROM &pool_table.
	GROUP BY indus_name, end_Date;
QUIT;
PROC TRANSPOSE DATA = stat OUT = stat(drop = _NAME_);
	BY indus_name;
	VAR nobs;
	ID end_Date;
RUN;

%cal_dist(input_table=&pool_table., by_var=indus_name, cal_var=research_pct, out_table=stat);
%cal_dist(input_table=&pool_table., by_var=year, cal_var=research_pct, out_table=stat);
%cal_dist(input_table=&pool_table., by_var=year indus_name, cal_var=research_pct, out_table=stat);


