/*** ר�������� */
/*** ������Դ��
(1) ר������: fgtest.test_zl3(�ñ�����Ѱ���stock_code���й���) --> zl_change(����att_year+1���ǹ�Ʊ�����ɵ�����)
����zl_change�Ĺ���:
product.zlsj --> zljs_subset --> zljs_subset_expand --> zl_change����Ƶ��
����create_zl_month�Ĺ��̣�
create_zl --> create_zl_month: ����Ƶ��
(2) ���¼�����Ʊ��: gxjs_stock_pool (����: gxjs_stock_pool������Ʊ�ع��˺�Ľ��)
**/

/** ���Ĳ���:
(1) һЩר����ͳ������
(2) ��gxjs_stock_pool���кϲ�������union_pool
***/

PROC SQL;
	CREATE TABLE zlsj AS
	SELECT ��Ʊ���� AS stock_code, 
			����� AS an,
			������ AS pn,
			������ AS apply_date,
			������ AS pub_date,
			���� AS name,
			������� AS main_class,
			������ AS applicant,
			������ AS inventor,
			ר������ AS ptype,
			table_sn AS table_sn, 
/*			ժҪ AS abstract,*/
			����״̬ AS law_date,
			ҳ�� AS pages,
			ר��Ȩ״̬ AS status,
			��� AS group_code,
			��������� AS country
	FROM fgtest.test_zl3
	ORDER BY stock_code, pn;
QUIT;
DATA product.zlsj(rename = (apply_date2 = apply_date pub_date2 = pub_date));
	SET zlsj;
	apply_date = trim(compress(apply_date,"."));
	pub_date = trim(compress(pub_date,"."));
	apply_date2 = input(apply_date, yymmdd8.);
	pub_date2 = input(pub_date, yymmdd8.);
	FORMAT apply_date2 pub_date2 yymmdd10.;
	drop apply_date pub_date;
	tail = substr(pn, length(pn),1);
	IF tail IN ("B","C") THEN my_status = 2; /** �ڶ��ι��� */
	ELSE my_status = 1;
	IF ptype IN ("1","8") THEN my_type = 1;  /** ����ר�� */
	ELSE IF ptype IN ("2","9") THEN my_type = 2; /** ʵ������ */
	ELSE my_type = 3; /* ������ */
RUN;


/** (!!���) ͳ��1��ÿ����������ר������ */
/** ����ר��ռ�У�70% */
PROC SQL;
	CREATE TABLE stat AS
	SELECT year(pub_date) AS year, month(pub_date) AS month, 
	sum(my_type=1 and my_status=2) AS fm2,
	sum(my_type=1 and my_status=1) AS fm1,
	sum(my_type=2 and my_status=1) AS sy1,
	sum(my_type=3 and my_status=1) AS wg1,
	count(1) AS nobs
	FROM product.zlsj
	GROUP BY year, month;
QUIT;

/** (!!�����ͳ��2��ͳ��ÿ����ר�����ݵ����й�˾���� */
/** ƽ��ÿ������130�����й�˾����ר������ */
PROC SQL;
	CREATE TABLE stat AS
	SELECT year(pub_date) AS year, month(pub_date) AS month, 
	count(distinct stock_code) AS nstock
	FROM product.zlsj
	WHERE pub_date >= "31dec2010"d
	GROUP BY year, month;
QUIT;


/** (��Ҫ!!) ͳ��3�����й�˾�������һ�η�����ʱ���� */
/*** �����������������й�˾�ڹ�ȥ�����·��Ѿ�����ר������ **/
DATA zlsj_subset;
	SET product.zlsj;
	WHERE pub_date >= "15dec2008"d;
RUN;
%get_month_date(busday_table=busday, start_date=15dec2008, end_date=30jun2015, rename=date, output_table=month_busdate, type=1);
/*%adjust_date_modify(busday_table=busday , raw_table=zlsj_subset ,colname=pub_date,  output_table=zlsj_subset, is_forward = 1);*/
%adjust_date_to_mapdate(rawdate_table=zlsj_subset, mapdate_table=month_busdate, 
	raw_colname=pub_date, map_colname=date, 
	output_table=zlsj_subset,is_backward=0, is_included=1);

PROC SQL;
	CREATE TABLE daily_zl AS
	SELECT stock_code, map_pub_date, count(1) AS nobs, 
		sum(my_status=1) AS nobs1, sum(my_status=2) AS nobs2,
		sum(my_type=1 AND my_status=1) AS nobs_fm,
		sum(my_type=2 AND my_status=1) AS nobs_sy,
		sum(my_type=3 AND my_status=1) AS nobs_wg
	FROM zlsj_subset
	GROUP BY stock_code, map_pub_Date;
QUIT;
PROC SORT DATA = month_busdate;
	BY date;
RUN;
DATA month_busdate;
	SET month_busdate;
	month_id = _N_;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.month_id
	FROM daily_zl A LEFT JOIN month_busdate B
	ON A.map_pub_date = B.date;
QUIT;
PROC SQL;
	CREATE TABLE daily_zl AS
	SELECT A.*, B.month_id AS prev_month_id
	FROM tmp A LEFT JOIN tmp B
	ON A.stock_code = B.stock_code AND A.month_id > B.month_id
	WHERE A.map_pub_date >= "15dec2010"d
	GROUP BY A.stock_code, A.month_id
	HAVING B.month_id = max(B.month_id);
QUIT;

DATA daily_zl;
	SET daily_zl;
	dif = month_id - prev_month_id;
RUN;
PROC SQL;
	CREATE TABLE stat AS
	SELECT dif, count(1) AS nobs
	FROM daily_zl
	GROUP BY dif;
QUIT;
DATA daily_zl;
	SET daily_zl;
	year = year(map_pub_date);
RUN;
PROC SORT DATA = daily_zl;
	BY descending nobs;
RUN;
PROC SORT DATA = daily_zl;
	BY year;
RUN;

DATA daily_zl_subset;
	SET daily_zl;
	IF map_pub_date >= "15dec2010"d;
RUN;
PROC SORT DATa = daily_zl_subset;
	BY year;
RUN;

/** (!!���) ͳ��4�����й�˾ÿ�귢����ר������ */
/** �в��ֹ�Ʊ�������ר�������ǳ���������ͨѶ(000063)������������Ʊ��������3���� */
PROC UNIVARIATE DATA = daily_zl_subset NOPRINT;
	BY year;
	VAR nobs;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;

/** ͳ��5: ��ͬ��ҵ�ڣ�����ÿ�·�����ר������ */
/** ���е�һ����ҵ��: TMT / �����豸 / ��е /�������� */
DATA daily_zl_subset;
	SET daily_zl_subset;
	end_date = map_pub_date;
	FORMAT end_date yymmdd10.;
RUN;
%get_sector_info(stock_table=daily_zl_subset, mapping_table=fg_wind_sector, output_stock_table=daily_zl_subset);
PROC SORT DATA = daily_zl_subset;
	BY indus_name;
RUN;

PROC UNIVARIATE DATA = daily_zl_subset NOPRINT;
	BY indus_name;
	VAR nobs;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;
PROC SQL;
	CREATE TABLE stat AS
	SELECT indus_name, count(distinct stock_code) AS nstock
	FROM daily_zl_subset
	GROUP BY indus_name
	ORDER BY nstock desc;
QUIT;


/** (!!���) ͳ��6: �����պ͹�����֮���ʱ�����  */
/** ������ʵ�����ͺ���۵ĵ�һ�ι������ڣ����������յ���λ����180-200��֮��*/
/** ��ſ�Խ�����ʱ�� */
DATA tt;
	SET zlsj_subset;
	dif = pub_date - apply_date;
	IF map_pub_date >= "15dec2010"d;
RUN;
PROC SORT DATA = tt;
	BY ptype my_status;
RUN;
PROC UNIVARIATE DATA = tt NOPRINT;
	BY ptype my_status;
	VAR dif;
	OUTPUT OUT = stat N = obs mean = mean std = std pctlpts = 100 90 75 50 25 10 0
	pctlpre = p;
QUIT;



/** ����Ҫ!!!��ͳ��7���������� */
/** �ṩ���ּ��裺
(����1) �з�����Ϊ���꣺��������[ȥ��7��1�գ�����6��30(map_pub_date)]֮�����Ϊ��ȥ���з����õĳɹ� -> ��6/30��Ϊ�ֽ���
(����2) �з����ڳɹ���Ϊ�Ǽ�ʱ�ģ���������[ȥ��1��1�գ�ȥ��12��31��]֮�����Ϊ��ȥ���з����õĳɹ� -> ��12/31��Ϊ�ֽ��� 
����ת���� = �ڼ�ר���� / �з�����
���⣬��Ϊ���з�����6�µ׵��������������ͬ����ʱ�����ת���ʣ�Ҫ�������ڽ���6/30��֮ǰ�ɵõģ����ܼ��㡣
****/

DATA zlsj_subset_expand;
	SET zlsj_subset;
	IF map_pub_date >= "15dec2010"d;
	/** ����1 **/
	IF apply_date <= mdy(6,30,year(apply_date)) THEN att_year = year(apply_date)-1;
	ELSE att_year = year(apply_date);
	/** ����2 **/
/*	att_year = year(apply_date);*/
	/** ���ݿɵã�Ҫ������һ���6-30֮ǰ�ܵõ� */
	IF map_pub_date < mdy(7,1,att_year+1) THEN is_get = 1;
	ELSE is_get = 0;
RUN;

/** �������(att_year)ͳ�Ƹ���ר����� */
PROC SQL;
	CREATE TABLE zl_change AS
	SELECT stock_code, att_year, att_year+1 AS year,
		sum(is_get=1) AS n_get,  /** �Դ���Ϊɸѡ��׼ **/
		sum(is_get=0) AS n_notget,
		sum(is_get=1 and my_status=1) AS n_first_get,
		sum(is_get=1 and my_status=2) AS n_second_get,
		sum(is_get=1 and my_status=1 and my_type=1) AS n_fm,
		sum(is_get=1 and my_status=1 and my_type=2) AS n_sy,
		sum(is_get=1 and my_status=1 and my_type=3) AS n_wg
	FROM zlsj_subset_expand
	GROUP BY stock_code, att_year;
QUIT;

/** ͳ��8: �����й�˾Ϊ��λͳ���������͵�ר�����������*/
/** �����������Ƿ�ɵ�,���޳��˷���ר����Ȩ(my_status=2) */
PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all
	FROM zlsj_subset_expand
	GROUP BY year, stock_code;
QUIT;
/** ר������λ����2011������������10-15������ */
%cal_dist(input_table=stat, by_var=year, cal_var=n_all, out_table=stat2);

/** ���ǿɵ��� */
PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all,
		count(1) AS n_all2
	FROM zlsj_subset_expand
	WHERE is_get = 1
	GROUP BY year, stock_code;
QUIT;
/** ר������λ����2011������������10-15������ */
%cal_dist(input_table=stat, by_var=year, cal_var=n_all, out_table=stat2);
%cal_dist(input_table=stat, by_var=year, cal_var=n_all2, out_table=stat2);


/** Ϊ�˱�ע���й�˾ר�����,������������:
(1) ȫ����
(2) ��ר����>=10
***/
%LET zl_var = n_fm;
DATA stat;
	SET stat;
	IF n_all = 0 THEN delete;
	IF n_all >=10 THEN mark = 1;
	ELSE mark = 0;
	IF &zl_var./n_all = 1 THEN g = 1;
	ELSE IF &zl_var./n_all >0.8 THEN g =2;
	ELSE IF &zl_var./n_all >0.5 THEN g= 3;
	ELSE IF  &zl_var./n_all > 0 THEN g = 4;
	ELSE g = 5;
RUN;
PROC SQL;
	CREATE TABLE stat2 AS
	SELECT year, g, count(1) AS nobs
	FROM stat
/*	WHERE mark = 1*/
	GROUP BY year, g;
QUIT;
PROC TRANSPOSE DATA = stat2 prefix = g OUT = stat2(drop = _NAME_);
	BY year;
	ID g;
	WHERE year >= 2012;
	VAR nobs;
RUN;


/** ͳ��9: ͳ���������͵����й�˾��ҵ�ֲ���*/
PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all
	FROM zlsj_subset_expand
	GROUP BY year, stock_code;
QUIT;
DATA stat;
	SET stat;
	IF n_all = 0 THEN delete;
	IF n_all > 0 THEN DO;
		IF n_fm/n_all > 0.5 THEN g = 1;
		ELSE IF n_sy/n_all > 0.5 THEN g = 2;
		ELSE IF n_wg/n_all > 0.5 THEN g = 3;
		ELSE g = 4;
	END;
	end_date = mdy(6,30,year);
	FORMAT end_date mmddyy10.;
RUN;
%get_sector_info(stock_table=stat, mapping_table=fg_wind_sector, output_stock_table=stat);

PROC SQL;
	CREATE TABLE stat2 AS
	SELECT year, g, count(1) AS nobs
	FROM stat
	WHERE year >= 2012
	GROUP BY year, g;
QUIT;
PROC TRANSPOSE DATA = stat2 prefix = g OUT = stat2(drop = _NAME_);
	BY year;
	ID g;
	VAR nobs;
RUN;

%LET g_group = 1;
PROC SQL;
	CREATE TABLE stat2 AS
	SELECT year, indus_name, count(1) AS nobs
	FROM stat
	WHERE g = &g_group. AND year >= 2012
	GROUP BY indus_name, year;
QUIT;
PROC TRANSPOSE DATA = stat2 prefix = y OUT = stat2(drop = _NAME_);
	BY indus_name;
	ID year;
	VAR nobs;
RUN;
DATA stat2(drop = i);
	SET stat2;
	ARRAY var_list(4) y2012-y2015;
	DO i = 1 TO 4;
		IF missing(var_list(i)) THEN var_list(i) = 0;
	END;
RUN; 


/** ͳ��10: ���й�˾ÿ���Ƿ���һ������������ר���ɹ���*/
 PROC SQL;
	CREATE TABLE stat AS
	SELECT att_year+1 AS year, stock_code,
		sum(my_status=1 and my_type=1) AS n_fm,
		sum(my_status=1 and my_type=2) AS n_sy,
		sum(my_status=1 and my_type=3) AS n_wg,
		sum(my_status=1) AS n_all
	FROM zlsj_subset_expand
	GROUP BY stock_code, year;
QUIT;
DATA stat;
	SET stat;
	IF n_all = 0 THEN delete;
RUN;
PROC TRANSPOSE DATA = stat prefix = y OUT = stat(drop = _NAME_);
	BY stock_code;
	ID year;
	VAR n_all;
	WHERE year >= 2011;
RUN;
DATA stat(drop = i);
	SET stat;
	ARRAY var_list(5) y2011-y2015;
	ARRAY var_list2(5) y2011_m y2012_m y2013_m y2014_m y2015_m;
	DO i = 1 TO 5;
		IF missing(var_list(i)) THEN var_list(i) = 0;
		IF var_list(i) > 0 THEN var_list2(i) = 1;
		ELSE var_list2(i) = 0;
	END;
RUN; 
DATA stat(drop = i);
	SET stat;
	ARRAY var_list2(4) m1 m2 m3 m4;
	ARRAY var_list(5) y2011_m y2012_m y2013_m y2014_m y2015_m;
	DO i = 2 TO 5;
		IF var_list(i-1) = 1 AND var_list(i) = 1 THEN var_list2(i-1) = 1;
		ELSE IF var_list(i-1) = 0 AND var_list(i) = 0 THEN var_list2(i-1) = 2;
		ELSE IF var_list(i-1) = 0 AND var_list(i) = 1 THEN var_list2(i-1) = 3;
		ELSE IF var_list(i-1) = 1 AND var_list(i) = 0 THEN var_list2(i-1) = 4;
	END;
RUN; 
%LET m_group = m4;
PROC SQL;
	CREATE TABLE stat2 AS
	SELECT &m_group., count(1) AS nobs
	FROM stat
	GROUP BY &m_group.;
QUIT;


/** ͳ��11: ���й�˾��ÿ������ר���������򣬼���֮��������(���ǹ����¶����ӵĿ�����)��*/

%MACRO cal_coef_add(coef_var, output_table);
	PROC SORT DATA = daily_zl_subset;
		BY stock_code;
	RUN;
	DATA tt;
		SET daily_zl_subset;
		KEEP map_pub_date stock_code &coef_var.;
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT A.*, coalesce(C.&coef_var.,0) AS &coef_var., 		
		year(A.map_pub_date)*10000+month(A.map_pub_date)*100+day(A.map_pub_date) AS date_num,
		A.map_pub_date AS end_Date FORMAT yymmdd10.
		FROM
		(
		SELECT map_pub_date, stock_code 
		FROM
		(SELECT distinct map_pub_date FROM tt) ,
		(SELECT distinct stock_code FROM tt) 
		) A 
		LEFT JOIN tt C
		ON A.map_pub_date = C.map_pub_date AND A.stock_code = C.stock_code
		ORDER BY A.stock_code, date_num;
	QUIT;
	PROC TRANSPOSE DATA = tmp prefix = d OUT = results;
		BY stock_code;
		ID date_num;
		VAR &coef_var.;
	RUN;
	
	%LET date_list = .;
	%LET ndate =.;
	%gen_macro_var_list(input_table=tmp, var_name=date_num, var_macro=date_list, nobs_macro=ndate);
	%put &date_list.;
	%put &ndate.;
	%DO i = 2 %TO &ndate.;
		%LET j = %sysevalf(&i.-1);
		%LET prev_month = %SCAN(&date_list., &j., " ");
		%LET cur_month = %SCAN(&date_list., &i., " ");
		DATA results;
			SET results;
			end_date = &cur_month.;
		RUN;
		%cal_coef(data=results, var1=d&prev_month., var2=d&cur_month., output_s = corr_s, output_p = corr_p);
		PROC SQL;
			CREATE TABLE stat AS
			SELECT A.end_Date, A.p_ic, B.s_ic
			FROM corr_p A JOIN corr_s B
			ON A.end_date = B.end_Date;
		QUIT;
		%IF %SYSEVALF(&i.=2) %THEN %DO;
			DATA &output_table.;
				SET stat;
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. stat;
			RUN;
		%END;
	%END;
	PROC SQL;
		DROP TABLE stat, tmp, results, corr_s, corr_p;
	QUIT;

%MEND cal_coef_add;
%cal_coef_add(nobs1, zl_results);




/************* ���������ĺ���(�¶�) ***************/

%get_month_date(busday_table=busday, start_date=15dec2011, end_date=30jun2015, rename=end_date, output_table=month_busdate, type=1);
DATA create_zl;
	SET month_busdate;
	IF end_date <= mdy(6,30,year(end_date)) THEN report_year = year(end_date)-2;
	ELSE report_year = year(end_date)-1;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.end_date, A.report_year,
		B.stock_code,
		B.pn,
		B.apply_date,
		B.pub_date,
		B.my_status,
		B.my_type,
		B.map_pub_date,
		B.status
	FROM create_zl A LEFT JOIN zlsj_subset B
	ON B.apply_date <= mdy(6,30,A.report_year+1)
	  AND B.apply_date > mdy(6,30,A.report_year)
      AND pub_date < end_date
	ORDER BY A.end_date, B.apply_date;
QUIT;
DATA create_zl;
	SET tmp;
RUN;
 
/** ͳ��ÿ���¶ȣ����ܵı�����ݵ�ר������(����ʱ������ƣ��ᷢ��һ���ı仯) */
PROC SQL;
	CREATE TABLE create_zl_month AS
	SELECT end_date, stock_code,report_year,
		sum(my_status=1 and my_type=1 and status ~="20") AS n_fm,
		sum(my_status=1 and my_type=2 and status ~= "20") AS n_sy,
		sum(my_status=1 and my_type=3 and status ~= "20") AS n_wg,
		sum(my_status=1 and status ~= "20") AS n_all,
		sum(my_status=2 and status ~= "20") AS n_fm_pub
	FROM create_zl
	GROUP BY end_date, stock_code, report_year
	ORDER BY stock_code, end_Date;
QUIT;

/** ��Ʊ��������� */
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.*
	FROM create_zl_month A LEFT JOIN product.report_data B
	ON A.stock_code = B.stock_code
	ORDER BY A.end_date, A.stock_code;
QUIT;
%MACRO add_report_data();
	%DO i = 2009 %TO 2014;
		DATA tmp;
			SET tmp;
			IF report_year = &i. THEN DO;
				research = research%sysevalf(&i.);
				assets = assets%sysevalf(&i.);
				revenue = revenue%sysevalf(&i.);
			END;
		RUN;
	%END;
%MEND add_report_data;
%add_report_data();
DATA create_zl_month(drop = assets2009--research2014);
	SET tmp;
	IF not missing(revenue) AND revenue ~=0 THEN DO;
		zl_value_fm = n_fm*100000000/revenue;
		zl_value_all = n_all*100000000/revenue;
	END;
	ELSE DO;
		zl_value_fm = .;
		zl_value_all = .;
	END;
RUN;
%cal_dist(input_table=create_zl_month, by_var=end_date, cal_var=zl_value_fm, out_table=stat);
%cal_dist(input_table=create_zl_month, by_var=end_date, cal_var=zl_value_all, out_table=stat);

/** ���ǵĹ�Ʊ���� */
PROC SQL;
	CREATE TABLE stat AS
	SELECT distinct stock_code
	FROM create_zl_month;
QUIT;



/************* �¼����(�¶�) ***************/

DATA zlsj_subset;
	SET product.zlsj;
	WHERE pub_date >= "15dec2008"d;
RUN;
%get_month_date(busday_table=busday, start_date=15dec2008, end_date=30jun2015, rename=date, output_table=month_busdate, type=1);
%adjust_date_to_mapdate(rawdate_table=zlsj_subset, mapdate_table=month_busdate, 
	raw_colname=pub_date, map_colname=date, 
	output_table=zlsj_subset,is_backward=0, is_included=1);

PROC SQL;
	CREATE TABLE zl_factor AS
	SELECT stock_code, map_pub_date, count(1) AS nobs, 
		sum(my_status=1 and my_type=1 and status ~="20") AS n_fm,
		sum(my_status=1 and my_type=2 and status ~= "20") AS n_sy,
		sum(my_status=1 and my_type=3 and status ~= "20") AS n_wg,
		sum(my_status=1 and status ~= "20") AS n_all,
		sum(my_status=2 and status ~= "20") AS n_fm_pub,
		sum(status = "20") AS n_fail
	FROM zlsj_subset
	GROUP BY stock_code, map_pub_Date;
QUIT;
PROC SORT DATA = month_busdate;
	BY date;
RUN;
DATA month_busdate;
	SET month_busdate;
	month_id = _N_;
RUN;
PROC SQL;
	CREATE TABLE tmp AS
	SELECT A.*, B.month_id, B.date AS end_date FORMAT yymmdd10.
	FROM zl_factor A LEFT JOIN month_busdate B
	ON A.map_pub_date = B.date
	WHERE map_pub_date >= "15dec2010"d;
QUIT;
DATA zl_factor;
	SET tmp;
	drop map_pub_date;
RUN;


/** �������� */
%MACRO past_var(varname, varname_mdf, start_month, end_month, is_include=1); 
	%IF %SYSEVALF(&is_include.=1) %THEN %DO;	
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, coalesce(C.&varname.,0) AS &varname._tmp
			FROM zl_factor A 
			LEFT JOIN zl_factor C
			ON A.stock_code = C.stock_code AND A.month_id - &end_month. <= C.month_id <= A.month_id - &start_month. 
			ORDER BY A.stock_code, A.end_date;
		QUIT;
		/** ���ȱʧ������Ϊ���·�Ϊ0����Ϊtmp�л�û�иü�¼���������ֵ������ڳ����·�����������ֱ��ʹ��mean*/
		PROC SQL;
			CREATE TABLE tmp2 AS
			SELECT A.*, B.&varname_mdf.
			FROM zl_factor A
			LEFT JOIN
			(SELECT stock_code,month_id, sum(&varname._tmp)/(&end_month.-(&start_month.)+1) AS  &varname_mdf.
			FROM tmp
			GROUP BY stock_code, month_id) B
			ON A.stock_code = B.stock_code AND A.month_id = B.month_id
			ORDER BY A.stock_code, A.month_id;
		QUIT;
	%END;

	%ELSE %DO;
		PROC SQL;
			CREATE TABLE tmp AS
			SELECT A.*, coalesce(C.&varname.,0) AS &varname._tmp
			FROM zl_factor A 
			LEFT JOIN zl_factor C
			ON A.stock_code = C.stock_code AND A.month_id - &end_month. <= C.month_id < A.month_id - &start_month. 
			ORDER BY A.stock_code, A.end_date;
		QUIT;
		PROC SQL;
			CREATE TABLE tmp2 AS
			SELECT A.*, B.&varname_mdf.
			FROM zl_factor A
			LEFT JOIN
			(SELECT stock_code,month_id, sum(&varname._tmp)/(&end_month.-(&start_month.)) AS &varname_mdf.
			FROM tmp
			GROUP BY stock_code, month_id) B
			ON A.stock_code = B.stock_code AND A.month_id = B.month_id
			ORDER BY A.stock_code, A.month_id;
		QUIT;
	%END;
	DATA zl_factor;
		SET tmp2;
		IF missing( &varname_mdf.) THEN  &varname_mdf. = 0;
	RUN;

		
	PROC SQL;
		DROP TABLE tmp, tmp2;
	QUIT;
%MEND past_var;

/** ������Ҫ������ͬ��ר������ */
%LET varname = n_all;
%past_var(varname = &varname., varname_mdf = nobs_prev0, start_month=0, end_month=0, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev1, start_month=1, end_month=1, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev2, start_month=2, end_month=2, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev3, start_month=3, end_month=3, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev4, start_month=4, end_month=4, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev5, start_month=5, end_month=5, is_include=1);
%past_var(varname =  &varname., varname_mdf = nobs_prev6, start_month=6, end_month=6, is_include=1);



/** �˹������ӹ� */
DATA zl_factor;
	SET zl_factor;
	/** ���ݵ�ǰ�·ݵľ���ֵ */
	cur_zl = nobs_prev0;
	/** ��ȥ��������������ֵ */
	cur_zl_t3 = nobs_prev0+nobs_prev1+nobs_prev2;
	/** ��ȥ��������������ֵ */
	cur_zl_t6 = nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5;
	/* ��ǰ�·���Թ�ȥ�����µľ���ֵ */
	cur_zl_dif3 = nobs_prev0 - (nobs_prev0+nobs_prev1+nobs_prev2)/3;
	/* ��ǰ�·���Թ�ȥ6���µľ���ֵ */
	cur_zl_dif6 = nobs_prev0 - (nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5)/6;
	/* ��ȥ��������Թ�ȥ6���µľ���ֵ */
	cur_zl_dif36 = (nobs_prev0+nobs_prev1+nobs_prev2)/3 - (nobs_prev0+nobs_prev1+nobs_prev2+nobs_prev3+nobs_prev4+nobs_prev5)/6;
RUN;

