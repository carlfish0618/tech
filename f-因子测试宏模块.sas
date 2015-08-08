/** 因子测试宏模块 */

/** Step1: 测试因子ic **/
%macro test_factor_ic(pool_table, fname);
/** 1- 原始因子值 */
	DATA test_pool(keep = end_date stock_code &fname.);
		SET &pool_table.;
	RUN;
	PROC SQL;
		CREATE TABLE stat AS
		SELECT end_date, sum(not missing(&fname.))/count(1) AS pct
		FROM test_pool
		GROUP BY end_date;
	QUIT;
	/** backup */
	DATA product.&fname._cover;
		SET stat;
	RUN;
	%output_to_excel(excel_path=&output_dir.\&fname..xls, input_table=stat, sheet_name = pct);
	
	/** Step3-1: 因子IC **/
	%single_factor_ic(factor_table=test_pool, return_table=ot2, group_name=stock_code, fname=&fname., type=3);
	/* backup */
	DATA product.&fname._ic;
		SET &fname._stat;
	RUN;
	%output_to_excel(excel_path=&output_dir.\&fname..xls, input_table=&fname._stat, sheet_name = ic);
	PROC SQL;
		DROP TABLE &fname._stat;
	QUIT;
%mend test_factor_ic;

%MACRO 	test_multiple_factor_ic(input_table, exclude_list);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
          %test_factor_ic(&input_table., &fname.);
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;
%MEND test_multiple_factor_ic;

/*** 汇总因子ic/coverage的结果，方便Excel查看 */
%MACRO merge_timeseries(merge_var, suffix, output_table, exclude_list, is_hit=0, factor_table=product.stock_pool_znw2);
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		&merge_var. LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
          PROC SQL;
			CREATE TABLE tmp AS
			SELECT "&fname." AS factor, 
			mean(&merge_var.) AS &merge_var._mean,
			sum(&merge_var.>0)/sum(not missing(&merge_var.)) AS &merge_var._hit
			FROM product.&fname._&suffix.
		QUIT;
		%IF %SYSEVALF (&is_hit.=0) %THEN %DO;
			DATA &output_table.;
				SET &output_table. tmp(rename = (&merge_var._mean = &merge_var.) drop =&merge_var._hit );
			RUN;
		%END;
		%ELSE %DO;
			DATA &output_table.;
				SET &output_table. tmp(rename = (&merge_var._hit = &merge_var.) drop =&merge_var._mean);
			RUN;
		%END;	
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;

%MEND merge_timeseries;

%MACRO construct_index(test_pool, bm_index_table, output_index_table, output_stat_table, output_trade_table, excel_path,  
			sheet_name_index, sheet_name_stat, sheet_name_trade, start_date=30jun2012, end_date=27jul2015, is_output=1);
	%neutralize_weight(stock_pool=&test_pool., output_stock_pool=&test_pool.);
	%gen_daily_pool(stock_pool=&test_pool., test_period_table=test_busdate, 
		adjust_date_table=adjust_busdate, output_stock_pool=&test_pool.);
	%cal_stock_wt_ret(daily_stock_pool=&test_pool., adjust_date_table=adjust_busdate, output_stock_pool=&test_pool.);
	%cal_portfolio_ret(daily_stock_pool=&test_pool., output_daily_summary=&output_index_table.);
	%eval_pfmance(index_pool=&output_index_table., bm_pool=&bm_index_table., index_ret=daily_ret, 
		bm_ret=daily_ret, start_date=&start_date., end_date=&end_date., type=1, output_table=&output_stat_table.);
	/** 统计换手率 */
	%trading_summary(daily_stock_pool=&test_pool., adjust_date_table=adjust_busdate,
		output_stock_trading=tt, output_daily_trading=&output_trade_table., trans_cost = 0.0035, type = 1);
	DATA &output_trade_table.;
		SET &output_trade_table.;
		year = year(date);
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT 0 AS year, sum(sell_wt)/(count(sell_wt)-1)*12 AS turnover /* 最开始的卖出为0，不予计算 */
		FROM &output_trade_table.;

		CREATE TABLE tmp2 AS
		SELECT year, sum(sell_wt) AS turnover
		FROM &output_trade_table.
		GROUP BY year;
	QUIT;
	DATA tmp;
		SET tmp tmp2;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.turnover
		FROM &output_stat_table. A LEFT JOIN tmp B
		ON A.year = B.year
		ORDER BY A.year;
	QUIT;
	DATA &output_stat_table.;
		SET tmp2;
	RUN;
	/** 统计股票数量*/
	DATA &output_index_table.;
		SET &output_index_table.;
		year = year(date);
	RUN;
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT 0 AS year, mean(nstock) AS nstock /* 最开始的卖出为0，不予计算 */
		FROM &output_index_table.;

		CREATE TABLE tmp2 AS
		SELECT year, mean(nstock) AS nstock
		FROM &output_index_table.
		GROUP BY year;
	QUIT;
	DATA tmp;
		SET tmp tmp2;
	RUN;
	PROC SQL;
		CREATE TABLE tmp2 AS
		SELECT A.*, B.nstock
		FROM &output_stat_table. A LEFT JOIN tmp B
		ON A.year = B.year
		ORDER BY A.year;
	QUIT;
	DATA &output_stat_table.;
		SET tmp2;
	RUN;
	/** 输出*/
	%IF %SYSEVALF(&is_output.=1) %THEN %DO;
		%output_to_excel(excel_path=&excel_path., input_table=&output_index_table., sheet_name = &sheet_name_index.);
		%output_to_excel(excel_path=&excel_path., input_table=&output_stat_table., sheet_name = &sheet_name_stat.);
		%output_to_excel(excel_path=&excel_path., input_table=&output_trade_table., sheet_name = &sheet_name_trade.);
	%END;
%MEND construct_index;


/** 分N组，等权 */
%MACRO test_factor_group_ret(pool_table, fname, ngroup =3, is_cut=1);
	DATA test_pool(keep = end_date stock_code &fname.);
		SET &pool_table.;
		IF not missing(&fname.);
	RUN;
	/** 分成N组 */
	%single_factor_score(raw_table=test_pool, identity=stock_code, factor_name=&fname.,
		output_table=test_pool, is_increase = 1, group_num = &ngroup.);
	%IF %SYSEVALF(&is_cut.=1) %THEN %DO;
		%LET test_var = &fname._score;
	%END;
	%ELSE %DO;
		%LET test_var = &fname.;
	%END; 

	PROC SQL NOPRINT;
		SELECT distinct &test_var., count(distinct &test_var.) 
		INTO :test_var_list SEPARATED BY ' ',
			 :ntest_var
		FROM test_pool;
	QUIT;
	
	%DO index = 1 %TO &ntest_var.;
		%LET rank = %scan(&test_var_list., &index., ' ');
		PROC SQL;
			CREATE TABLE test_stock_pool AS
			SELECT end_date, stock_code, 1 AS weight
			FROM test_pool
			WHERE &test_var. = &rank.;
		QUIT;
		%construct_index(test_pool=test_stock_pool, bm_index_table=bm_equal, 
			output_index_table=product.&fname._g&rank._index,
			output_stat_table=product.&fname._g&rank._stat,
			output_trade_table=product.&fname._g&rank._trade,
			excel_path=&output_dir.\&fname..xls, 
			sheet_name_index = index_g&rank.,
			sheet_name_stat = stat_g&rank.,
			sheet_name_trade = trade_g&rank.,
			start_date=&test_start_date., end_date=&test_end_Date.);
	%END;
%MEND test_factor_group_ret;


/** 取排名最高的min(100,half(N))，等权和得分加权 */
/** 二者都是以流通市值组合，作为基准 */
%MACRO test_factor_higher_group_ret(pool_table, fname, nstock=100, bm_table=bm_weight);
	DATA test_pool(keep = end_date stock_code &fname.);
		SET &pool_table.;
		IF not missing(&fname.);
	RUN;
	/** Step1: 取排名最高的前100名(或一半)*/
	%single_factor_score(raw_table=test_pool, identity=stock_code, factor_name=&fname.,
		output_table=test_pool, is_increase = 1, group_num = 2);
	%cut_subset(input_table=test_pool, colname=&fname., output_table=test_pool, type=3, threshold=&nstock., is_decrease=1, is_cut=0);

	/** 有一些因为精度问题，看似大于0，但其视为0。之后在得分加权的组合中，其权重实质上为0 */
	PROC SQL;
		CREATE TABLE tmp AS
		SELECT *
		FROM test_pool 
		WHERE cut_mark = 1 AND &fname. > 0 AND abs(&fname.) >= 0.00001 AND 
		&fname. >= (SELECT min(&fname.) FROM test_pool WHERE &fname._score = 2);
	QUIT;
	DATA test_pool;
		SET tmp;
	RUN;

	/** Step2: 等权组合 */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT end_date, stock_code, 1 AS weight
		FROM test_pool
	QUIT;
	%construct_index(test_pool=test_stock_pool, bm_index_table=&bm_table., 
		output_index_table=product.&fname._&nstock._index_e,
		output_stat_table=product.&fname._&nstock._stat_e,
		output_trade_table=product.&fname._&nstock._trade_e,
		excel_path=&output_dir.\&fname..xls, 
		sheet_name_index = index_&nstock._e,
		sheet_name_stat = stat_&nstock._e,
		sheet_name_trade = trade_&nstock._e,
		start_date=&test_start_date., end_date=&test_end_Date.);


	/** Step3: 得分加权组合 */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT end_date, stock_code, &fname. AS weight
		FROM test_pool;
	QUIT;
/*	%get_stock_size(stock_table=test_stock_pool, info_table=hqinfo, share_table=fg_wind_freeshare,*/
/*		output_table=test_stock_pool, colname=weight, index = 1);*/

	%construct_index(test_pool=test_stock_pool, bm_index_table=&bm_table., 
		output_index_table=product.&fname._&nstock._index_w,
		output_stat_table=product.&fname._&nstock._stat_w,
		output_trade_table=product.&fname._&nstock._trade_w,
		excel_path=&output_dir.\&fname..xls, 
		sheet_name_index = index_&nstock._w,
		sheet_name_stat = stat_&nstock._w,
		sheet_name_trade = trade_&nstock._w,
		start_date=&test_start_date., end_date=&test_end_Date.);


%MEND test_factor_higher_group_ret;

/** 适合于特定离散变量的 */
%MACRO test_factor_higher_group_ret2(pool_table, fname, fname_value, nstock=100, bm_table=bm_weight);
	DATA test_pool(keep = end_date stock_code &fname.);
		SET &pool_table.;
		IF not missing(&fname.) AND &fname.=&fname_value.;
	RUN;

	/** Step2: 等权组合 */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT end_date, stock_code, 1 AS weight
		FROM test_pool
	QUIT;
	%construct_index(test_pool=test_stock_pool, bm_index_table=&bm_table., 
		output_index_table=product.&fname._&nstock._index_e,
		output_stat_table=product.&fname._&nstock._stat_e,
		output_trade_table=product.&fname._&nstock._trade_e,
		excel_path=&output_dir.\&fname..xls, 
		sheet_name_index = index_&nstock._e,
		sheet_name_stat = stat_&nstock._e,
		sheet_name_trade = trade_&nstock._e,
		start_date=&test_start_date., end_date=&test_end_Date.);


	/** Step3: 得分加权组合 */
	PROC SQL;
		CREATE TABLE test_stock_pool AS
		SELECT end_date, stock_code, &fname. AS weight
		FROM test_pool;
	QUIT;
/*	%get_stock_size(stock_table=test_stock_pool, info_table=hqinfo, share_table=fg_wind_freeshare,*/
/*		output_table=test_stock_pool, colname=weight, index = 1);*/

	%construct_index(test_pool=test_stock_pool, bm_index_table=&bm_table., 
		output_index_table=product.&fname._&nstock._index_w,
		output_stat_table=product.&fname._&nstock._stat_w,
		output_trade_table=product.&fname._&nstock._trade_w,
		excel_path=&output_dir.\&fname..xls, 
		sheet_name_index = index_&nstock._w,
		sheet_name_stat = stat_&nstock._w,
		sheet_name_trade = trade_&nstock._w,
		start_date=&test_start_date., end_date=&test_end_Date.);


%MEND test_factor_higher_group_ret2;



%MACRO 	test_multiple_factor_group_ret(input_table, exclude_list, is_cut=1, bm_table=bm_weight);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
/*          %test_factor_group_ret(&input_table., &fname.,is_cut=&is_cut.);*/
		  %test_factor_higher_group_ret(&input_table., &fname., bm_table=&bm_table.);

     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;
%MEND test_multiple_factor_group_ret;

%MACRO 	test_multiple_factor_group_ret2(input_table, exclude_list, is_cut=1, bm_table=bm_weight);
	DATA rr_result;
		SET &input_table.;
	RUN;

	PROC CONTENTS DATA = &input_table. OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
/*          %test_factor_group_ret(&input_table., &fname.,is_cut=&is_cut.);*/
		  %test_factor_higher_group_ret2(&input_table., &fname.,3, bm_table=&bm_table.);

     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;
%MEND test_multiple_factor_group_ret2;

/*** 汇总所有因子分三组的结果，方便Excel查看 */
%MACRO merge_result(merge_var, output_table, exclude_list, factor_table=product.stock_pool_znw2);
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		g1_&merge_var. LENGTH = 8
		g2_&merge_var. LENGTH = 8
		g3_&merge_var. LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
	/* 判断是否存在 */
    %let ds_flag = %sysfunc(open(product.&fname._g1_stat,is));  
    %if &ds_flag. eq 0 %then %do;  
		DATA product.&fname._g1_stat;
			ATTRIB
			year LENGTH = 8
			g1_&merge_var. LENGTH = 8
			;
			STOP;
		RUN;
    %end;  

	 %let ds_flag = %sysfunc(open(product.&fname._g2_stat,is));  
     %if &ds_flag. eq 0 %then %do;  
		DATA product.&fname._g2_stat;
			ATTRIB
			year LENGTH = 8
			g2_&merge_var. LENGTH = 8
			;
			STOP;
		RUN;
    %end;  

	 %let ds_flag = %sysfunc(open(product.&fname._g3_stat,is));  
     %if &ds_flag. eq 0 %then %do;  
		DATA product.&fname._g3_stat;
			ATTRIB
			year LENGTH = 8
			g3_&merge_var. LENGTH = 8
			;
			STOP;
		RUN;
    %end;  

           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
          PROC SQL;
			CREATE TABLE tmp AS
			SELECT "&fname." AS factor, 
			A.&merge_var. AS g1_&merge_var.,
			B.&merge_var. AS g2_&merge_var.,
			C.&merge_var. AS g3_&merge_var.
			FROM product.&fname._g1_stat A 
			LEFT JOIN product.&fname._g2_stat B
			ON A.year = B.year
			LEFT JOIN product.&fname._g3_stat C
			ON A.year = C.year
			WHERE A.year = 0;
		QUIT;
		DATA &output_table.;
			SET &output_table. tmp;
		RUN;
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;

%MEND merge_result;

/*** 汇总所有因子取前N名的结果，方便Excel查看 */
%MACRO merge_result_higher_total(input_pre, year, merge_var, output_table, exclude_list, factor_table=product.stock_pool_znw2);
	DATA &output_table.;
		ATTRIB
		factor LENGTH = $30.
		&merge_var. LENGTH = 8
		;
		STOP;
	RUN;
	DATA rr_result;
		SET &factor_table.;
	RUN;

	PROC CONTENTS DATA = rr_result OUT = tt_varlist2(keep = name) NOPRINT;
	RUN;
	DATA tt_varlist2;
		SET tt_varlist2;
		IF upcase(name) NOT IN ("END_DATE", "STOCK_CODE","FMV_SQR");
		IF upcase(name) NOT IN &exclude_list.;
	RUN;
	
	PROC SQL NOPRINT;
		SELECT name, count(1)
		 INTO :name_list2 SEPARATED BY ' ',
               :nfactors2
          FROM tt_varlist2;
     QUIT;
           
     %DO i = 1 %TO &nfactors2.;
          %LET fname =  %scan(&name_list2.,&i., ' ');
		  %put testing &fname....;
		  DATA tmp(drop = year);
		  	SET product.&fname._&input_pre.(keep = year &merge_var.);
			LENGTH factor $ 30.;
			factor = "&fname.";
			IF year = &year.;
		  RUN;
		 DATA &output_table.;
			SET &output_table. tmp;
		 RUN;
     %END;
	PROC SQL;
		DROP TABLE rr_result, tt_varlist2;
	QUIT;

%MEND merge_result_higher_total;
