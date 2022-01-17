#CODE FOR GEOGRAPHY LEVEL RETAIL SALES INTERMEDIATE
# RAW INGESTION OF DATA FROM L2 ALIGNED SALES FOR WEEKLY AND MONTHLY XPO AND PLAN DYNAMICS DATA SOURCES
# APPLICATION OF TIME BUCKET AND FILTERING FOR RELEVANT BUCKETS FOR DIFFERENT DATA SOURCES
# APPLICATION OF EPA AND FILTERING OF TEAMS REPORTED IN MATRIX
# ROLL UP DATA TILL BRAND FAMILY
# APPLICATION OF PDRP RULE
# APPLICATION OF BLOCKING RULES
# PROFILE INFO WITH SEGMENTATION AND SPECIALTY
#ALL TABLES ARE CREATED IN PARQUET FORMAT WITH SNAPPY COMPRESSION
#spark.conf.set('hive.optimize.bucketmapjoin','true')
#spark.conf.set('hive.optimize.bucketmapjoin.sortedmerge','true')
#spark.conf.set('spark.sql.shuffle.partitions', '600')
#spark.conf.set('spark.default.parallelism','2000')
#spark.conf.set("spark.sql.autoBroadcastJoinThreshold",-1)
#spark.conf.set('spark.shuffle.spill.compress', 'true')
#spark.conf.set('spark.shuffle.compress', 'true')
#us_commercial_datalake_app_matrix_$$env
#us_commercial_datalake_app_commons_$$env
# Last changed By : Anjali Pathak
#Last Change Date: 31-03-2020
#Reason for change : RENAL LTC - Channel enhancement for pulling LTC,Retail/MO for Renal Teams and ALL(Retai/Mail + LTC) Data to be consumed for rest of the teams

spark.sql("""set sources.commitProtocolClass=com.databricks.io.CommitProtocol""")
spark.sql("""set spark.sql.broadcastTimeout = 30000""")
spark.sql(""" set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true") 
spark.conf.set("spark.sql.shuffle.partitions", 3200) 

#CREATION AND INGESTION OF EPA UNIVERSE INTO A DATAFRAME

#spark.sql(""" CREATE  TABLE temp_epa_uvrs
#(
#geo_id STRING
#)
#PARTITIONED BY (az_mkt_id STRING)
#STORED AS PARQUET
#TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY')
#""")

spark.conf.set('hive.exec.dynamic.partition.mode',"nonstrict")

temp_epa_uvrs=spark.sql(""" 
SELECT
geo_id ,
az_mkt_id
FROM
r_aligt_mkt_epa_uvrs_tempdf where az_mkt_id $$az_mkt_id
group by
geo_id ,
az_mkt_id
""")
temp_epa_uvrs.registerTempTable("temp_epa_uvrs")

#spark.sql("cache table temp_epa_uvrs").show()

#Factor dimension
temp_fctr_mkt=spark.sql("""
select /*+ BROADCAST(cfg) */
eff_dt,
end_dt,
fctr_type,
az_mkt_id,
mkt_nm,
az_prod_id,
nrx_ct_fctr,
nrx_unit_fctr,
trx_ct_fctr,
trx_unit_fctr,
nbrx_ct_fctr
from
temp_itmd_iqvia_fctr_dim_tempdf fctr
join  cfg_gner_parm_val_tempdf cfg
where az_mkt_id $$az_mkt_id and
lower(cfg.parm_type)='market'
and
lower(cfg.tbl_nm)='iqvia_fctr_dim'
and
lower(fctr.mkt_nm)=lower(cfg.val)
""")

temp_fctr_mkt.createOrReplaceTempView("temp_fctr_mkt")


temp_pfs_mkt_brd=spark.sql(""" 
select /*+ BROADCAST(cfg) */ az_brd_id,az_mkt_id
from
d_prod_tempdf brd
join  cfg_gner_parm_val_tempdf cfg
where az_mkt_id $$az_mkt_id and
lower(cfg.parm_type)='market'
and
lower(cfg.tbl_nm)='iqvia_pfs_market'
and
lower(brd.mkt_nm)=lower(cfg.val)
group by az_brd_id,az_mkt_id
""")

temp_pfs_mkt_brd.registerTempTable("temp_pfs_mkt_brd")

spark.sql(""" drop table if exists temp_pfs_mkt """)
temp_pfs_mkt=spark.sql("""
select distinct az_mkt_id
from
temp_pfs_mkt_brd
""")

temp_pfs_mkt.registerTempTable("temp_pfs_mkt")

#R2'20 Filter for blocking LTC channel for Renal teams
cfg_ltc_blk = spark.sql("""select val,attr1 
from cfg_gner_parm_val_tempdf
where tbl_nm = 'renal_ltc_block'
and parm_type = 'team_nm'""")

cfg_ltc_blk.createOrReplaceTempView("cfg_ltc_blk")


#INGESTION OF SALES XPO WEEKLY AND APPLICATION OF EPA UNIVERSE TO FILTER TEAM AND BRAND COMBINATIONS

temp_XPO_WKLY_EPA1=spark.sql("""
select /*+ broadcast(EPAB,b,cfg) */
az_cust_id,
a.az_geo_id,
a.az_prod_id,
a.az_brd_id,
a.az_mkt_id,
a.az_prod_ind,
az_plan_id,
chnl_id,
chnl_desc,
TRANSLATE(sls_dt,'-', '') as sls_dt,
src_sys_sk,
src_prod_grp_id,
src_sys_psbr_id,
az_team_id,
team_nm,
nrx,
nrx_units,
nrx_factrd_units,
trx,
trx_units,
trx_factrd_units,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_out_ind as pdrp_opt_sta,
ama_no_contact_ind as ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
pdrp_opt_out_ind,
ama_no_contact_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
pueblo_mkt_sna_ind,
legal_ind,
aspen_ind,
dns_ind,
case when cust_clas in ('OTH') then 'N' else non_eval_ind end as non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
brd_nm as az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
data_dt as load_dt
FROM
f_sls_hcp_geo_prod_plan_wk_tempdf a
INNER JOIN
temp_epa_uvrs EPAB
ON
a.az_geo_id=EPAB.GEO_ID
AND a.az_mkt_id=EPAB.az_mkt_id
left join 
temp_pfs_mkt b
on a.az_mkt_id=b.az_mkt_id
LEFT JOIN 
	cfg_ltc_blk cfg 
ON 
	a.chnl_desc = cfg.val AND a.team_nm = cfg.attr1
where a.az_mkt_id $$az_mkt_id and prod_lvl<>'PFS' and  b.az_mkt_id is null
AND (cfg.val is null OR cfg.attr1 is null) 
""")

temp_XPO_WKLY_EPA1.registerTempTable("temp_XPO_WKLY_EPA1")

temp_XPO_WKLY_EPA2=spark.sql(""" 
select /*+ broadcast(EPAB,cfg,b) */
az_cust_id,
a.az_geo_id,
a.az_prod_id,
a.az_brd_id,
a.az_mkt_id,
a.az_prod_ind,
az_plan_id,
chnl_id,
chnl_desc,
TRANSLATE(sls_dt,'-', '') as sls_dt,
src_sys_sk,
src_prod_grp_id,
src_sys_psbr_id,
az_team_id,
team_nm,
nrx,
nrx_units,
nrx_factrd_units,
trx,
trx_units,
trx_factrd_units,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_out_ind as pdrp_opt_sta,
ama_no_contact_ind as ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
pdrp_opt_out_ind,
ama_no_contact_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
pueblo_mkt_sna_ind,
legal_ind,
aspen_ind,
dns_ind,
case when cust_clas in ('OTH') then 'N' else non_eval_ind end as non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
brd_nm as az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
data_dt as load_dt
FROM
f_sls_hcp_geo_prod_plan_wk_tempdf a
INNER JOIN
temp_epa_uvrs EPAB
ON
a.az_geo_id=EPAB.GEO_ID
AND a.az_mkt_id=EPAB.az_mkt_id
left join 
temp_pfs_mkt_brd b
on a.az_prod_id=b.az_brd_id and
a.az_mkt_id=b.az_mkt_id 
LEFT JOIN 
	cfg_ltc_blk cfg 
ON 
	a.chnl_desc = cfg.val AND a.team_nm = cfg.attr1
where b.az_brd_id is not null and b.az_mkt_id is not null
AND (cfg.val is null OR cfg.attr1 is null)""")

temp_XPO_WKLY_EPA2.registerTempTable("temp_XPO_WKLY_EPA2")

table_list=spark.sql("""show tables in default""")
table_name=table_list.filter(table_list.tableName=="temp_XPO_WKLY_EPA_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_WKLY_EPA_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_WKLY_EPA_$$mktsplit""")
else:
    print("table not found")

temp_XPO_WKLY_EPA_$$mktsplit=spark.sql("""
select * from temp_XPO_WKLY_EPA1
union all
select * from temp_XPO_WKLY_EPA2
""")

temp_XPO_WKLY_EPA_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_WKLY_EPA_$$mktsplit")

spark.sql("""refresh table default.temp_XPO_WKLY_EPA_$$mktsplit""")

src_sys_nm_df=spark.sql("""select /*+ broadcast(cfg) */ distinct src_sys_sk,cfg.attr1 as mkt_nm
from
d_src_sys_tempdf d
JOIN
cfg_gner_parm_val_tempdf cfg
ON
d.src_sys_nm = cfg.val
where cfg.tbl_nm='XPO_PD_SRC_SYS' and cfg.PARM_TYPE='SRC_SYS' """)

src_sys_nm_df.registerTempTable("src_sys_nm_df")

#INGESTION OF SALES PLN DYN WKLY AND APPLICATION OF EPA UNIVERSE TO FILTER TEAM AND BRAND COMBINATIONS

temp_PLN_DYN_WKLY_EPA1=spark.sql("""
SELECT /*+ broadcast(EPAB,C,cfg,e) */
az_cust_id,
a.az_geo_id,
az_prod_id,
az_prod_ind,
a.az_brd_id,
a.az_mkt_id,
az_plan_id,
chnl_id,
chnl_desc,
TRANSLATE(sls_dt,'-', '') as sls_dt,
A.src_sys_sk,
src_prod_grp_id,
src_sys_psbr_id,
az_team_id,
team_nm,
A.mkt_nm,
nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_out_ind as pdrp_opt_sta,
ama_no_contact_ind as ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
pdrp_opt_out_ind,
ama_no_contact_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
pueblo_mkt_sna_ind,
legal_ind,
aspen_ind,
dns_ind,
case when cust_clas in ('OTH') then 'N' else non_eval_ind end as non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
brd_nm as az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
data_dt as load_dt
FROM
f_sls_hcp_geo_prod_plan_dyn_wk_tempdf A
JOIN
temp_epa_uvrs EPAB
ON
A.AZ_GEO_ID=EPAB.GEO_ID
AND A.AZ_MKT_ID=EPAB.AZ_MKT_ID
JOIN
src_sys_nm_df C
on A.src_sys_sk=C.src_sys_sk
and
A.mkt_nm=C.mkt_nm
left join 
temp_pfs_mkt e
on A.AZ_MKT_ID=e.az_mkt_id
LEFT JOIN 
	cfg_ltc_blk cfg 
ON 
	A.chnl_desc = cfg.val AND A.team_nm = cfg.attr1
where A.az_mkt_id $$az_mkt_id and prod_lvl<>'PFS' and  e.az_mkt_id is null
AND (cfg.val is null OR cfg.attr1 is null) 
""")

temp_PLN_DYN_WKLY_EPA1.registerTempTable("temp_PLN_DYN_WKLY_EPA1")

temp_PLN_DYN_WKLY_EPA2=spark.sql("""  
SELECT /*+ broadcast(EPAB,C,cfg,f) */
az_cust_id,
a.az_geo_id,
az_prod_id,
az_prod_ind,
a.az_brd_id,
a.az_mkt_id,
az_plan_id,
chnl_id,
chnl_desc,
TRANSLATE(sls_dt,'-', '') as sls_dt,
A.src_sys_sk,
src_prod_grp_id,
src_sys_psbr_id,
az_team_id,
team_nm,
A.mkt_nm,
nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_out_ind as pdrp_opt_sta,
ama_no_contact_ind as ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
pdrp_opt_out_ind,
ama_no_contact_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
pueblo_mkt_sna_ind,
legal_ind,
aspen_ind,
dns_ind,
case when cust_clas in ('OTH') then 'N' else non_eval_ind end as non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
brd_nm as az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
data_dt as load_dt
FROM
f_sls_hcp_geo_prod_plan_dyn_wk_tempdf A
JOIN
temp_epa_uvrs EPAB
ON
A.AZ_GEO_ID=EPAB.GEO_ID
AND A.AZ_MKT_ID=EPAB.AZ_MKT_ID
JOIN
src_sys_nm_df C
on A.src_sys_sk=C.src_sys_sk
and
A.mkt_nm=C.mkt_nm
left join 
temp_pfs_mkt_brd f
on a.az_prod_id=f.az_brd_id
AND
a.az_mkt_id=f.az_mkt_id
LEFT JOIN 
	cfg_ltc_blk cfg 
ON 
	A.chnl_desc = cfg.val AND A.team_nm = cfg.attr1
where A.az_mkt_id $$az_mkt_id and f.az_brd_id is not null
AND (cfg.val is null OR cfg.attr1 is null) 
""")


temp_PLN_DYN_WKLY_EPA2.registerTempTable("temp_PLN_DYN_WKLY_EPA2")

table_list=spark.sql("""show tables in default""")
table_name=table_list.filter(table_list.tableName=="temp_PLN_DYN_WKLY_EPA_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_PLN_DYN_WKLY_EPA_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_PLN_DYN_WKLY_EPA_$$mktsplit""")
else:
    print("table not found")

temp_PLN_DYN_WKLY_EPA_$$mktsplit=spark.sql("""
select * from temp_PLN_DYN_WKLY_EPA1
union all
select * from temp_PLN_DYN_WKLY_EPA2
""")

temp_PLN_DYN_WKLY_EPA_$$mktsplit.write.mode("overwrite").saveAsTable("temp_PLN_DYN_WKLY_EPA_$$mktsplit")
spark.sql("""refresh table default.temp_PLN_DYN_WKLY_EPA_$$mktsplit""")

temp_MKT_CFG_MTHLY=spark.sql("""SELECT VAL FROM cfg_gner_parm_val_tempdf WHERE TBL_NM='temp_XPO_MTHLY_TB' and PARM_TYPE='MKT'""")
temp_MKT_CFG_MTHLY.registerTempTable("temp_MKT_CFG_MTHLY")

temp_epa_uvrs_df=spark.sql("""SELECT GEO_ID,AZ_MKT_ID FROM temp_epa_uvrs WHERE AZ_MKT_ID in (SELECT /*+ BROADCAST(temp_MKT_CFG_MTHLY) */ VAL FROM temp_MKT_CFG_MTHLY)""")

temp_epa_uvrs_df.createOrReplaceTempView("temp_epa_uvrs_df")
#spark.sql("cache table temp_epa_uvrs_df").show()
'''
#INGESTION OF SALES XPO MTHLY AND APPLICATION OF EPA UNIVERSE TO FILTER TEAM AND BRAND COMBINATIONS
spark.sql(""" drop table if exists temp_XPO_MTHLY_EPA""")
spark.sql("""
CREATE  TABLE temp_XPO_MTHLY_EPA STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS
SELECT /*+ broadcast(EPAB) */
az_cust_id,
a.az_geo_id,
a.az_prod_id,
az_prod_ind,
a.az_brd_id,
a.az_mkt_id,
az_plan_id,
chnl_id,
TRANSLATE(sls_dt,'-', '') as sls_dt,
src_sys_sk,
src_prod_grp_id,
src_sys_psbr_id,
az_team_id,
team_nm,
nrx,
nrx_units,
nrx_factrd_units,
trx,
trx_units,
trx_factrd_units,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_out_ind as pdrp_opt_sta,
ama_no_contact_ind as ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
pdrp_opt_out_ind,
ama_no_contact_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
pueblo_mkt_sna_ind,
legal_ind,
aspen_ind,
dns_ind,
case when cust_clas in ('OTH') then 'N' else non_eval_ind end as non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
brd_nm as az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
data_dt as load_dt
FROM us_commercial_datalake_app_commons_$$env.f_sls_hcp_geo_prod_plan_mnth    A
JOIN
temp_epa_uvrs_df EPAB
ON
A.AZ_GEO_ID=EPAB.GEO_ID
AND A.AZ_MKT_ID=EPAB.AZ_MKT_ID
where prod_lvl<>'PFS' """)

#INGESTION OF SALES PLN DYN MTHLY AND APPLICATION OF EPA UNIVERSE TO FILTER TEAM AND BRAND COMBINATIONS

spark.sql(""" drop table if exists temp_PLN_DYN_MTHLY_EPA""")
spark.sql(""" CREATE  TABLE temp_PLN_DYN_MTHLY_EPA STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS
SELECT /*+ broadcast(EPAB) */
az_cust_id,
a.az_geo_id,
az_prod_id,
az_prod_ind,
az_brd_id,
a.az_mkt_id,
az_plan_id,
chnl_id,
TRANSLATE(sls_dt,'-', '') as sls_dt,
A.src_sys_sk,
src_prod_grp_id,
src_sys_psbr_id,
az_team_id,
team_nm,
nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_out_ind as pdrp_opt_sta,
ama_no_contact_ind as ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
pdrp_opt_out_ind,
ama_no_contact_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
pueblo_mkt_sna_ind,
legal_ind,
aspen_ind,
dns_ind,
case when cust_clas in ('OTH') then 'N' else non_eval_ind end as non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
brd_nm as az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
'20200404' as load_dt
FROM
us_commercial_datalake_app_commons_$$env.f_sls_hcp_geo_prod_plan_dyn_mnth     A
JOIN
temp_epa_uvrs_df EPAB
ON
A.AZ_GEO_ID=EPAB.GEO_ID
AND A.AZ_MKT_ID=EPAB.AZ_MKT_ID
JOIN
src_sys_nm_df C
on A.src_sys_sk=C.src_sys_sk
where prod_lvl<>'PFS'  """)
'''
#FILTERING THE MATRIX TIME BUCKET FOR XPO WEEKLY SOURCE AND CREATING A SMALL TABLE FOR DATA PROCESSING

temp_d_time_bckt_XPO_WKLY=spark.sql("""
SELECT appl_nm,
tb_id,
src_nm,
tb_ds,
tb_cd,
tb_type,
tb_freq,
trnd_ind,
strt_dt,
end_dt,
strt_dt_id,
end_dt_id,
data_dt as data_dt, 
load_dt,
cycl_id as cycl_id
from
d_time_bckt_tempdf
where src_nm='SLS' and appl_nm='MAT'
""")
temp_d_time_bckt_XPO_WKLY.registerTempTable("temp_d_time_bckt_XPO_WKLY")

#spark.sql("cache table temp_d_time_bckt_XPO_WKLY").show()

cal_wk_end_dt=spark.sql(""" 
select distinct SLS_DT as wk_end_dt from default.temp_PLN_DYN_WKLY_EPA_$$mktsplit
union
select distinct SLS_DT as wk_end_dt from default.temp_XPO_WKLY_EPA_$$mktsplit
""")
cal_wk_end_dt.registerTempTable("cal_wk_end_dt")
#spark.sql("cache table cal_wk_end_dt").show()

TB_WKLY_FLTR=spark.sql("""
select /*+ broadcast(b) */ A.*,wk_end_dt
from temp_d_time_bckt_XPO_WKLY A
JOIN
cal_wk_end_dt b
where wk_end_dt between
A.STRT_DT and A.END_DT
""")
TB_WKLY_FLTR.registerTempTable("TB_WKLY_FLTR")
#spark.sql(""" cache table TB_WKLY_FLTR""").show()

FCTR_WKLY_FLTR=spark.sql("""
select /*+ broadcast(b) */ A.*,wk_end_dt
from temp_fctr_mkt A
JOIN
cal_wk_end_dt b
where CAST(TRANSLATE(wk_end_dt,'-','') as string)  between cast(translate(to_date(A.eff_dt),'-','') as string)  and cast(translate(to_date(A.end_dt),'-','') as string)
""")

FCTR_WKLY_FLTR.registerTempTable("FCTR_WKLY_FLTR")

#spark.sql(""" cache table FCTR_WKLY_FLTR""").show()
'''
#FILTERING THE MATRIX TIME BUCKET FOR XPO MONTHLY INSULIN SOURCE AND CREATING A SMALL TABLE FOR DATA PROCESSING
spark.sql(""" drop table if exists temp_d_time_bckt_XPO_MTHLY""")
spark.sql(""" CREATE TABLE temp_d_time_bckt_XPO_MTHLY STORED AS PARQUET LOCATION '/user/hive/warehouse/matrix/temp_d_time_bckt_XPO_MTHLY' TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS
SELECT appl_nm,
tb_id,
src_nm,
tb_ds,
tb_cd,
tb_type,
tb_freq,
trnd_ind,
strt_dt,
end_dt,
strt_dt_id,
end_dt_id,
'20200404',
load_dt,
cycl_id
from
us_commercial_datalake_app_matrix_$$env.d_time_bckt
where src_nm='SLS_INSULIN' and appl_nm='MAT'
""")

cal_mnth_end_dt=spark.sql("""
select distinct SLS_DT as mnth_end_dt from temp_PLN_DYN_MTHLY_EPA
union
select distinct SLS_DT as mnth_end_dt from temp_XPO_MTHLY_EPA
""")

cal_mnth_end_dt.createOrReplaceTempView("cal_mnth_end_dt")

spark.sql(""" drop table if exists TB_MNTHLY_FLTR""")
spark.sql(""" CREATE TABLE TB_MNTHLY_FLTR STORED AS PARQUET as
select /*+ broadcast(cal_mnth_end_dt) */ A.*,mnth_end_dt
from temp_d_time_bckt_XPO_WKLY A
JOIN
cal_mnth_end_dt
where mnth_end_dt between
A.STRT_DT and A.END_DT
""")

#spark.sql("cache table TB_MNTHLY_FLTR").show()

spark.sql(""" drop table if exists FCTR_MNTHLY_FLTR""")
spark.sql(""" CREATE TABLE FCTR_MNTHLY_FLTR STORED AS PARQUET as
select /*+ broadcast(cal_mnth_end_dt) */ A.*,mnth_end_dt
from temp_fctr_mkt A
JOIN
cal_mnth_end_dt
where CAST(TRANSLATE(mnth_end_dt,'-','') as string)  between CAST(TRANSLATE(A.eff_dt,'-','') as string)  and CAST(TRANSLATE(A.end_dt,'-','') as string)
""")
#spark.sql(""" cache table FCTR_MNTHLY_FLTR""").show()
'''

#MERGING THE SOURCE SALES FACT XPO WEEKLY AND ROLLING UP DATA AT DIFFERENT TIME BUCKETS. TIME BUCKET HAS BEEN BROADCASTED FOR FASTER DATA ACCESS AND PROCESSING

table_name=table_list.filter(table_list.tableName=="temp_XPO_WKLY_TB_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_WKLY_TB_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_WKLY_TB_$$mktsplit""")
else:
    print("table not found")
    
temp_XPO_WKLY_TB_$$mktsplit=spark.sql("""
SELECT /*+ broadcast(MTB,FCTR) */
SLS.az_cust_id,
SLS.az_geo_id ,
SLS.az_prod_id,
SLS.az_brd_id,
SLS.az_prod_ind,
SLS.az_mkt_id,
SLS.az_plan_id,
SLS.chnl_id,
SLS.chnl_desc,
MTB.tb_id,
MTB.tb_cd,
MTB.tb_type,
SLS.src_sys_sk,
SLS.src_prod_grp_id,
SLS.src_sys_psbr_id,
SLS.az_team_id,
SLS.team_nm,
case when nrx_ct_fctr is null then SLS.nrx else (SLS.nrx * nrx_ct_fctr) end nrx,
SLS.nrx_units,
SLS.nrx_factrd_units,
case when nrx_unit_fctr is null then SLS.nrx_units else (SLS.nrx_units * nrx_unit_fctr) end nrx_eu_units,
case when trx_ct_fctr is null then SLS.trx else (SLS.trx * trx_ct_fctr) end trx,
SLS.trx_units,
SLS.trx_factrd_units,
case when trx_unit_fctr is null then SLS.trx_units else (SLS.trx_units * trx_unit_fctr) end trx_eu_units,
SLS.cust_clas,
SLS.cust_nm,
SLS.cust_typ,
SLS.cust_sta_cd,
SLS.pri_spec_cd,
SLS.mkt_spec_nm,
SLS.trgt_psbr_mkt_ind,
SLS.pdrp_opt_sta,
SLS.ama_sta,
SLS.ihc_ind,
SLS.kaiser_ind,
SLS.nosee_ind,
SLS.pdrp_opt_out_ind,
SLS.ama_no_contact_ind,
SLS.pueblo_mkt_exc_ind,
SLS.pueblo_mkt_blk_ind,
SLS.pueblo_mkt_sna_ind,
SLS.legal_ind,
SLS.aspen_ind,
SLS.dns_ind,
SLS.non_eval_ind ,
SLS.encumbered_psbr_ind,
SLS.purple_excl_ind,
SLS.95_excl_ind,
SLS.az_brd_nm,
SLS.plan_nm,
SLS.pay_typ,
SLS.az_bot_acct_id,
SLS.bot_acct_nm,
SLS.modl_var_typ,
SLS.frmy_tier,
SLS.frmy_sta,
SLS.frmy_posn,
SLS.medicaid_pln_blk,
SLS.tricare_pln_blk,
SLS.load_dt
FROM default.temp_XPO_WKLY_EPA_$$mktsplit SLS
JOIN
TB_WKLY_FLTR MTB
on SLS.SLS_DT=MTB.WK_END_DT
LEFT JOIN  FCTR_WKLY_FLTR FCTR
on SLS.SLS_DT=FCTR.WK_END_DT
and SLS.az_mkt_id=FCTR.az_mkt_id
and SLS.az_prod_id=FCTR.az_prod_id
""")
temp_XPO_WKLY_TB_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_WKLY_TB_$$mktsplit")
spark.sql("""refresh table default.temp_XPO_WKLY_TB_$$mktsplit""")

#MERGING THE SOURCE SALES FACT PLN DYN WEEKLY AND ROLLING UP DATA AT DIFFERENT TIME BUCKETS. TIME BUCKET HAS BEEN BROADCASTED FOR FASTER DATA ACCESS AND PROCESSING
table_name=table_list.filter(table_list.tableName=="temp_PLN_DYN_WKLY_TB_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_PLN_DYN_WKLY_TB_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_PLN_DYN_WKLY_TB_$$mktsplit""")
else:
    print("table not found")

temp_PLN_DYN_WKLY_TB_$$mktsplit=spark.sql("""select
/*+ broadcast(MTB,FCTR) */
SLS.az_cust_id,
SLS.az_geo_id,
SLS.az_prod_id,
SLS.az_brd_id,
SLS.az_prod_ind,
SLS.az_mkt_id,
SLS.az_plan_id,
SLS.chnl_id,
SLS.chnl_desc,
MTB.tb_id,
MTB.tb_cd,
MTB.tb_type,
SLS.src_prod_grp_id,
SLS.src_sys_psbr_id,
SLS.az_team_id,
SLS.team_nm,
case when nbrx_ct_fctr is null then SLS.nw_brd_rx else (SLS.nw_brd_rx * nbrx_ct_fctr) end nw_brd_rx,
SLS.cust_clas,
SLS.cust_nm,
SLS.cust_typ,
SLS.cust_sta_cd,
SLS.pri_spec_cd,
SLS.mkt_spec_nm,
SLS.trgt_psbr_mkt_ind,
SLS.pdrp_opt_sta,
SLS.ama_sta,
SLS.ihc_ind,
SLS.kaiser_ind,
SLS.nosee_ind,
SLS.pdrp_opt_out_ind,
SLS.ama_no_contact_ind,
SLS.pueblo_mkt_exc_ind,
SLS.pueblo_mkt_blk_ind,
SLS.pueblo_mkt_sna_ind,
SLS.legal_ind,
SLS.aspen_ind,
SLS.dns_ind,
SLS.non_eval_ind,
SLS.encumbered_psbr_ind,
SLS.purple_excl_ind,
SLS.95_excl_ind,
SLS.az_brd_nm,
SLS.plan_nm,
SLS.pay_typ,
SLS.az_bot_acct_id,
SLS.bot_acct_nm,
SLS.modl_var_typ,
SLS.frmy_tier,
SLS.frmy_sta,
SLS.frmy_posn,
SLS.medicaid_pln_blk,
SLS.tricare_pln_blk,
SLS.load_dt
FROM
default.temp_PLN_DYN_WKLY_EPA_$$mktsplit SLS
JOIN
TB_WKLY_FLTR MTB
on SLS.SLS_DT=MTB.WK_END_DT
LEFT JOIN  FCTR_WKLY_FLTR FCTR
on SLS.SLS_DT=FCTR.WK_END_DT
and SLS.az_mkt_id=FCTR.az_mkt_id
and SLS.az_prod_id=FCTR.az_prod_id
""")

temp_PLN_DYN_WKLY_TB_$$mktsplit.write.mode("overwrite").saveAsTable("temp_PLN_DYN_WKLY_TB_$$mktsplit")
spark.sql("""refresh table default.temp_PLN_DYN_WKLY_TB_$$mktsplit""")
'''
#MERGING THE SOURCE SALES FACT XPO MONTHLY AND ROLLING UP DATA AT DIFFERENT TIME BUCKETS. TIME BUCKET HAS BEEN BROADCASTED FOR FASTER DATA ACCESS AND PROCESSING
spark.sql(""" drop table if exists temp_XPO_MTHLY_TB""")
spark.sql(""" CREATE TABLE temp_XPO_MTHLY_TB STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS SELECT /*+ BROADCAST(temp_d_time_bckt_XPO_MTHLY) */
SLS.az_cust_id,
SLS.az_geo_id ,
SLS.az_prod_id,
SLS.az_brd_id,
SLS.az_prod_ind,
SLS.az_mkt_id,
SLS.az_plan_id,
SLS.chnl_id,
MTB.tb_id,
MTB.tb_cd,
MTB.tb_type,
SLS.src_sys_sk,
SLS.src_prod_grp_id,
SLS.src_sys_psbr_id,
SLS.az_team_id,
SLS.team_nm,
case when nrx_ct_fctr is null then SLS.nrx else (SLS.nrx * nrx_ct_fctr) end nrx,
SLS.nrx_units,
SLS.nrx_factrd_units,
case when nrx_unit_fctr is null then SLS.nrx_units else (SLS.nrx_units * nrx_unit_fctr) end nrx_eu_units,
case when trx_ct_fctr is null then SLS.trx else (SLS.trx * trx_ct_fctr) end trx,
SLS.trx_units,
SLS.trx_factrd_units,
case when trx_unit_fctr is null then SLS.trx_units else (SLS.trx_units * trx_unit_fctr) end trx_eu_units,
SLS.cust_clas,
SLS.cust_nm,
SLS.cust_typ,
SLS.cust_sta_cd,
SLS.pri_spec_cd,
SLS.mkt_spec_nm,
SLS.trgt_psbr_mkt_ind,
SLS.pdrp_opt_sta,
SLS.ama_sta,
SLS.ihc_ind,
SLS.kaiser_ind,
SLS.nosee_ind,
SLS.pdrp_opt_out_ind,
SLS.ama_no_contact_ind,
SLS.pueblo_mkt_exc_ind,
SLS.pueblo_mkt_blk_ind,
SLS.pueblo_mkt_sna_ind,
SLS.legal_ind,
SLS.aspen_ind,
SLS.dns_ind,
SLS.non_eval_ind ,
SLS.encumbered_psbr_ind,
SLS.purple_excl_ind,
SLS.95_excl_ind,
SLS.az_brd_nm,
SLS.plan_nm,
SLS.pay_typ,
SLS.az_bot_acct_id,
SLS.bot_acct_nm,
SLS.modl_var_typ,
SLS.frmy_tier,
SLS.frmy_sta,
SLS.frmy_posn,
SLS.medicaid_pln_blk,
SLS.tricare_pln_blk,
SLS.load_dt
FROM temp_XPO_MTHLY_EPA SLS
JOIN
TB_MNTHLY_FLTR MTB
on SLS.SLS_DT=MTB.MNTH_END_DT 
LEFT JOIN  FCTR_MNTHLY_FLTR FCTR
on SLS.SLS_DT=FCTR.MNTH_END_DT
and SLS.az_mkt_id=FCTR.az_mkt_id
and SLS.az_prod_id=FCTR.az_prod_id""")

spark.sql("drop table if exists temp_XPO_MTHLY_EPA")

#MERGING THE SOURCE SALES FACT PLN DYN MONTHLY AND ROLLING UP DATA AT DIFFERENT TIME BUCKETS. TIME BUCKET HAS BEEN BROADCASTED FOR FASTER DATA ACCESS AND PROCESSING
spark.sql(""" drop table if exists temp_PLN_DYN_MTHLY_TB""")
spark.sql(""" CREATE TABLE temp_PLN_DYN_MTHLY_TB STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS
SELECT /*+ BROADCAST(temp_d_time_bckt_XPO_MTHLY) */
SLS.az_cust_id,
SLS.az_geo_id,
SLS.az_prod_id,
SLS.az_brd_id,
SLS.az_prod_ind,
SLS.az_mkt_id,
SLS.az_plan_id,
SLS.chnl_id,
MTB.tb_id,
MTB.tb_cd,
MTB.tb_type,
SLS.src_prod_grp_id,
SLS.src_sys_psbr_id,
SLS.az_team_id,
SLS.team_nm,
case when nbrx_ct_fctr is null then SLS.nw_brd_rx else (SLS.nw_brd_rx * nbrx_ct_fctr) end nw_brd_rx,
SLS.cust_clas,
SLS.cust_nm,
SLS.cust_typ,
SLS.cust_sta_cd,
SLS.pri_spec_cd,
SLS.mkt_spec_nm,
SLS.trgt_psbr_mkt_ind,
SLS.pdrp_opt_sta,
SLS.ama_sta,
SLS.ihc_ind,
SLS.kaiser_ind,
SLS.nosee_ind,
SLS.pdrp_opt_out_ind,
SLS.ama_no_contact_ind,
SLS.pueblo_mkt_exc_ind,
SLS.pueblo_mkt_blk_ind,
SLS.pueblo_mkt_sna_ind,
SLS.legal_ind,
SLS.aspen_ind,
SLS.dns_ind,
SLS.non_eval_ind,
SLS.encumbered_psbr_ind,
SLS.purple_excl_ind,
SLS.95_excl_ind,
SLS.az_brd_nm,
SLS.plan_nm,
SLS.pay_typ,
SLS.az_bot_acct_id,
SLS.bot_acct_nm,
SLS.modl_var_typ,
SLS.frmy_tier,
SLS.frmy_sta,
SLS.frmy_posn,
SLS.medicaid_pln_blk,
SLS.tricare_pln_blk,
SLS.load_dt
FROM
temp_PLN_DYN_MTHLY_EPA SLS
JOIN
TB_MNTHLY_FLTR MTB
on SLS.SLS_DT=MTB.MNTH_END_DT
LEFT JOIN  FCTR_MNTHLY_FLTR FCTR
on SLS.SLS_DT=FCTR.MNTH_END_DT
and SLS.az_mkt_id=FCTR.az_mkt_id
and SLS.az_prod_id=FCTR.az_prod_id  """)

spark.sql("drop table if exists temp_PLN_DYN_MTHLY_EPA")

spark.sql(""" INSERT INTO TABLE temp_XPO_WKLY_TB_$$mktsplit SELECT * FROM temp_XPO_MTHLY_TB""")

spark.sql(""" INSERT INTO TABLE temp_PLN_DYN_WKLY_TB_$$mktsplit SELECT * FROM temp_PLN_DYN_MTHLY_TB""")
'''
table_name=table_list.filter(table_list.tableName=="temp_XPO_WKLY_TB_AGGR_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_WKLY_TB_AGGR_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_WKLY_TB_AGGR_$$mktsplit""")
else:
    print("table not found")
    
temp_XPO_WKLY_TB_AGGR_$$mktsplit=spark.sql("""
SELECT
A.az_cust_id,
A.az_geo_id,
A.az_prod_id,
A.chnl_desc,
MAX(A.az_prod_ind) as az_prod_ind,
A.az_mkt_id,
A.az_plan_id,
min(A.tb_id) as tb_id,
A.tb_cd,
A.tb_type,
MAX(A.src_sys_psbr_id) as src_sys_psbr_id,
MAX(A.az_team_id) as AZ_TEAM_ID,
MAX(A.team_nm) as team_nm,
SUM(A.nrx) as nrx,
SUM(A.nrx_units) as nrx_units,
SUM(A.nrx_factrd_units) as nrx_factrd_units,
SUM(A.nrx_eu_units) as nrx_eu_units,
SUM(A.trx) as trx,
SUM(A.trx_units) as trx_units,
SUM(A.trx_factrd_units) as trx_factrd_units,
SUM(A.trx_eu_units) as trx_eu_units,
MAX(A.cust_clas) as cust_clas,
MAX(A.cust_nm ) as cust_nm,
MAX(A.cust_typ) as cust_typ,
MAX(A.cust_sta_cd) as cust_sta_cd,
MAX(A.pri_spec_cd) as pri_spec_cd,
MAX(A.mkt_spec_nm) as mkt_spec_nm,
MAX(A.trgt_psbr_mkt_ind) as trgt_psbr_mkt_ind,
MAX(A.pdrp_opt_sta) as pdrp_opt_sta,
MAX(A.ama_sta) as ama_sta,
MAX(A.ihc_ind) as ihc_ind,
MAX(A.kaiser_ind) as kaiser_ind,
MAX(A.nosee_ind) as nosee_ind,
MAX(A.ama_no_contact_ind) as ama_no_contact_ind,
MAX(A.pueblo_mkt_exc_ind) as pueblo_mkt_exc_ind,
MAX(A.pueblo_mkt_blk_ind) as pueblo_mkt_blk_ind,
MAX(A.pueblo_mkt_sna_ind) as pueblo_mkt_sna_ind,
MAX(A.legal_ind) as legal_ind,
MAX(A.aspen_ind) as aspen_ind,
MAX(A.dns_ind) as dns_ind,
MAX(A.non_eval_ind ) as non_eval_ind,
MAX(A.encumbered_psbr_ind) as encumbered_psbr_ind,
MAX(A.purple_excl_ind) as purple_excl_ind,
MAX(A.95_excl_ind) as 95_excl_ind,
MAX(A.az_brd_nm) as az_brd_nm,
MAX(A.plan_nm) as plan_nm,
A.pay_typ as pay_typ,
A.az_bot_acct_id as az_bot_acct_id,
MAX(A.bot_acct_nm) as bot_acct_nm,
MAX(A.modl_var_typ) as modl_var_typ,
MAX(A.frmy_tier ) as frmy_tier,
MAX(A.frmy_sta) as frmy_sta,
MAX(A.frmy_posn) as frmy_posn,
MAX(A.medicaid_pln_blk) as medicaid_pln_blk,
MAX(A.tricare_pln_blk) as tricare_pln_blk,
MAX(A.load_dt) as load_dt
FROM
default.temp_XPO_WKLY_TB_$$mktsplit A
GROUP BY
a.az_cust_id,
a.az_geo_id,
a.az_prod_id,
a.chnl_desc,
a.az_mkt_id,
a.az_plan_id,
a.pay_typ,
a.az_bot_acct_id,
a.tb_cd,
a.tb_type
""")

temp_XPO_WKLY_TB_AGGR_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_WKLY_TB_AGGR_$$mktsplit")

spark.sql("""refresh table default.temp_XPO_WKLY_TB_AGGR_$$mktsplit""")

table_name=table_list.filter(table_list.tableName=="temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit""")
else:
    print("table not found")
    
temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit=spark.sql("""
SELECT
A.az_cust_id,
A.az_geo_id,
A.az_prod_id,
A.chnl_desc,
MAX(A.az_prod_ind) as az_prod_ind,
A.az_mkt_id,
A.az_plan_id,
min(A.tb_id) as tb_id,
A.tb_cd,
A.tb_type,
MAX(A.src_sys_psbr_id) as src_sys_psbr_id,
MAX(A.az_team_id) as AZ_TEAM_ID,
MAX(A.team_nm) as team_nm,
SUM(A.nw_brd_rx) as nw_brd_rx,
MAX(A.cust_clas) as cust_clas,
MAX(A.cust_nm ) as cust_nm,
MAX(A.cust_typ) as cust_typ,
MAX(A.cust_sta_cd) as cust_sta_cd,
MAX(A.pri_spec_cd) as pri_spec_cd,
MAX(A.mkt_spec_nm) as mkt_spec_nm,
MAX(A.trgt_psbr_mkt_ind) as trgt_psbr_mkt_ind,
MAX(A.pdrp_opt_sta) as pdrp_opt_sta,
MAX(A.ama_sta) as ama_sta,
MAX(A.ihc_ind) as ihc_ind,
MAX(A.kaiser_ind) as kaiser_ind,
MAX(A.nosee_ind) as nosee_ind,
MAX(A.ama_no_contact_ind) as ama_no_contact_ind,
MAX(A.pueblo_mkt_exc_ind) as pueblo_mkt_exc_ind,
MAX(A.pueblo_mkt_blk_ind) as pueblo_mkt_blk_ind,
MAX(A.pueblo_mkt_sna_ind) as pueblo_mkt_sna_ind,
MAX(A.legal_ind) as legal_ind,
MAX(A.aspen_ind) as aspen_ind,
MAX(A.dns_ind) as dns_ind,
MAX(A.non_eval_ind ) as non_eval_ind,
MAX(A.encumbered_psbr_ind) as encumbered_psbr_ind,
MAX(A.purple_excl_ind) as purple_excl_ind,
MAX(A.95_excl_ind) as 95_excl_ind,
MAX(A.az_brd_nm) as az_brd_nm,
MAX(A.plan_nm) as plan_nm,
A.pay_typ as pay_typ,
A.az_bot_acct_id as az_bot_acct_id,
MAX(A.bot_acct_nm) as bot_acct_nm,
MAX(A.modl_var_typ) as modl_var_typ,
MAX(A.frmy_tier ) as frmy_tier,
MAX(A.frmy_sta) as frmy_sta,
MAX(A.frmy_posn) as frmy_posn,
MAX(A.medicaid_pln_blk) as medicaid_pln_blk,
MAX(A.tricare_pln_blk) as tricare_pln_blk,
MAX(A.load_dt) as load_dt
FROM
default.temp_PLN_DYN_WKLY_TB_$$mktsplit A
GROUP BY
a.az_cust_id,
a.az_geo_id,
a.az_prod_id,
a.chnl_desc,
a.az_mkt_id,
a.az_plan_id,
a.pay_typ,
a.az_bot_acct_id,
a.tb_id,
a.tb_cd,
a.tb_type
""")

temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit.write.mode("overwrite").saveAsTable("temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit")

spark.sql("""refresh table default.temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit""")

table_name=table_list.filter(table_list.tableName=="temp_XPO_PLN_DYN_WKLY_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_PLN_DYN_WKLY_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_PLN_DYN_WKLY_$$mktsplit""")
else:
    print("table not found")
    
temp_XPO_PLN_DYN_WKLY_$$mktsplit=spark.sql("""
SELECT
COALESCE(A.az_cust_id,B.az_cust_id) as az_cust_id,
COALESCE(A.az_geo_id,B.az_geo_id) as az_geo_id,
COALESCE(A.az_prod_id,B.az_prod_id) as az_prod_id,
COALESCE(A.az_prod_ind,B.az_prod_ind) as az_prod_ind,
COALESCE(A.az_mkt_id,B.az_mkt_id) as az_mkt_id,
COALESCE(A.az_plan_id,B.az_plan_id) as az_plan_id,
COALESCE(A.tb_id,B.tb_id) as tb_id,
COALESCE(A.tb_cd,B.tb_cd) as tb_cd,
COALESCE(A.tb_type,B.tb_type) as tb_type,
COALESCE(A.src_sys_psbr_id,B.src_sys_psbr_id) as src_sys_psbr_id,
COALESCE(A.AZ_TEAM_ID,B.AZ_TEAM_ID) as AZ_TEAM_ID,
COALESCE(A.team_nm,B.team_nm) as team_nm,
COALESCE(A.chnl_desc,B.chnl_desc) as chnl_desc,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
B.nw_brd_rx,
COALESCE(A.cust_clas,B.cust_clas) as cust_clas,
COALESCE(A.cust_nm,B.cust_nm) as cust_nm,
COALESCE(A.cust_typ,B.cust_typ) as cust_typ,
COALESCE(A.cust_sta_cd,B.cust_sta_cd) as cust_sta_cd,
COALESCE(A.pri_spec_cd,B.pri_spec_cd) as pri_spec_cd,
COALESCE(A.mkt_spec_nm,B.mkt_spec_nm) as mkt_spec_nm,
COALESCE(A.trgt_psbr_mkt_ind,B.trgt_psbr_mkt_ind) as trgt_psbr_mkt_ind,
COALESCE(A.pdrp_opt_sta,B.pdrp_opt_sta) as pdrp_opt_sta,
COALESCE(A.ama_sta,B.ama_sta) as ama_sta,
COALESCE(A.ihc_ind,B.ihc_ind) as ihc_ind,
COALESCE(A.kaiser_ind,B.kaiser_ind) as kaiser_ind,
COALESCE(A.nosee_ind,B.nosee_ind) as nosee_ind,
COALESCE(A.ama_no_contact_ind ,B.ama_no_contact_ind) as ama_no_contact_ind,
COALESCE(A.pueblo_mkt_sna_ind ,B.pueblo_mkt_sna_ind) as pueblo_mkt_sna_ind,
COALESCE(A.pueblo_mkt_exc_ind ,B.pueblo_mkt_exc_ind) as pueblo_mkt_exc_ind,
COALESCE(A.pueblo_mkt_blk_ind ,B.pueblo_mkt_blk_ind) as pueblo_mkt_blk_ind,
COALESCE(A.legal_ind ,B.legal_ind) as legal_ind,
COALESCE(A.aspen_ind ,B.aspen_ind ) as aspen_ind,
COALESCE(A.dns_ind ,B.dns_ind) as dns_ind,
COALESCE(A.non_eval_ind  ,B.non_eval_ind) as non_eval_ind,
COALESCE(A.encumbered_psbr_ind ,B.encumbered_psbr_ind) as encumbered_psbr_ind,
COALESCE(A.purple_excl_ind ,B.purple_excl_ind) as purple_excl_ind,
COALESCE(A.95_excl_ind ,B.95_excl_ind) as 95_excl_ind,
COALESCE(A.az_brd_nm ,B.az_brd_nm) as az_brd_nm,
COALESCE(A.plan_nm ,B.plan_nm) as plan_nm,
COALESCE(A.pay_typ ,B.pay_typ) as pay_typ,
COALESCE(A.az_bot_acct_id ,B.az_bot_acct_id) as  az_bot_acct_id,
COALESCE(A.bot_acct_nm,B.bot_acct_nm) as bot_acct_nm,
COALESCE(A.modl_var_typ,B.modl_var_typ) as modl_var_typ,
COALESCE(A.frmy_tier,B.frmy_tier) as frmy_tier,
COALESCE(A.frmy_sta,B.frmy_sta) as frmy_sta,
COALESCE(A.frmy_posn,B.frmy_posn )as frmy_posn,
COALESCE(A.medicaid_pln_blk,B.medicaid_pln_blk) as medicaid_pln_blk,
COALESCE(A.tricare_pln_blk,B.tricare_pln_blk) as tricare_pln_blk,
A.load_dt as load_dt
FROM
default.temp_XPO_WKLY_TB_AGGR_$$mktsplit A
FULL OUTER JOIN
default.temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit B
ON
A.az_cust_id=B.az_cust_id
and
A.az_geo_id=B.az_geo_id
and
A.az_prod_id=B.az_prod_id
and
A.az_mkt_id=B.az_mkt_id
and
A.az_plan_id=b.az_plan_id
and
A.tb_id=b.tb_id
and
A.chnl_desc=B.chnl_desc
""")

temp_XPO_PLN_DYN_WKLY_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_PLN_DYN_WKLY_$$mktsplit")
spark.sql("""refresh table default.temp_XPO_PLN_DYN_WKLY_$$mktsplit""")
#cfg pass for channel mapping
df_cfg_chnl = spark.sql("""select val , attr1 ,attr2
from cfg_gner_parm_val_tempdf where 
tbl_nm = 'renal_teams' and 
parm_type = 'all_channels'""")

df_cfg_chnl.createOrReplaceTempView("df_cfg_chnl")

#table for distinct chnl views for Renal teams (LTC and R/MO channel views)
table_name=table_list.filter(table_list.tableName=="tb_chnl_views_dif_r_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.tb_chnl_views_dif_r_$$mktsplit""")
    spark.sql("""drop table if exists default.tb_chnl_views_dif_r_$$mktsplit""")
else:
    print("table not found")
    
tb_chnl_views_dif_r_$$mktsplit=spark.sql("""
select /*+ BROADCAST(cfg) */
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
cfg.attr2 as chnl_nm,
nrx,
nrx_units,
nrx_factrd_units,
nrx_eu_units,
trx,
trx_units,
trx_factrd_units,
trx_eu_units,
nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
load_dt as load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from default.temp_XPO_PLN_DYN_WKLY_$$mktsplit A
join df_cfg_chnl cfg
on A.chnl_desc = cfg.attr1
and A.team_nm = cfg.val
""")

tb_chnl_views_dif_r_$$mktsplit.write.mode("overwrite").saveAsTable("tb_chnl_views_dif_r_$$mktsplit")
spark.sql("""refresh table default.tb_chnl_views_dif_r_$$mktsplit""")
#table for for aggregation of renal teams (LTC and REtal+MO group by)
table_name=table_list.filter(table_list.tableName=="tb_chnl_views_dif_agr_r_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.tb_chnl_views_dif_agr_r_$$mktsplit""")
    spark.sql("""drop table if exists default.tb_chnl_views_dif_agr_r_$$mktsplit""")
else:
    print("table not found")
    
tb_chnl_views_dif_agr_r_$$mktsplit=spark.sql("""
select 
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
chnl_nm,
sum(nrx) as nrx,
sum(nrx_units) as nrx_units,
sum(nrx_factrd_units) as nrx_factrd_units,
sum(nrx_eu_units) as nrx_eu_units,
sum(trx) as trx,
sum(trx_units) as trx_units,
sum(trx_factrd_units) as trx_factrd_units,
sum(trx_eu_units) as trx_eu_units,
sum(nw_brd_rx)as nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
max(load_dt) as load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from 
default.tb_chnl_views_dif_r_$$mktsplit 
group by 
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
chnl_nm,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk
""")

tb_chnl_views_dif_agr_r_$$mktsplit.write.mode("overwrite").saveAsTable("tb_chnl_views_dif_agr_r_$$mktsplit")
spark.sql("""refresh table default.tb_chnl_views_dif_agr_r_$$mktsplit""")
#tbl for chnl views ALL (All teams considered)
table_name=table_list.filter(table_list.tableName=="tb_chnl_views_dif_rnr_all_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.tb_chnl_views_dif_rnr_all_$$mktsplit""")
    spark.sql("""drop table if exists default.tb_chnl_views_dif_rnr_all_$$mktsplit""")
else:
    print("table not found")
    
tb_chnl_views_dif_rnr_all_$$mktsplit=spark.sql("""
select 
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
'All (Retail/Mail + LTC)' as chnl_nm,
sum(nrx) as nrx,
sum(nrx_units) as nrx_units,
sum(nrx_factrd_units) as nrx_factrd_units,
sum(nrx_eu_units) as nrx_eu_units,
sum(trx) as trx,
sum(trx_units) as trx_units,
sum(trx_factrd_units) as trx_factrd_units,
sum(trx_eu_units) as trx_eu_units,
sum(nw_brd_rx)as nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
max(load_dt) as load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from default.temp_XPO_PLN_DYN_WKLY_$$mktsplit 
group by
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk
""")

tb_chnl_views_dif_rnr_all_$$mktsplit.write.mode("overwrite").saveAsTable("tb_chnl_views_dif_rnr_all_$$mktsplit")
spark.sql("""refresh table default.tb_chnl_views_dif_rnr_all_$$mktsplit""")

#Tbl for Renal and ALL dataframes

tb_chnl_views_total2=spark.sql("""
select 
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
chnl_nm,
nrx,
nrx_units,
nrx_factrd_units,
nrx_eu_units,
trx,
trx_units,
trx_factrd_units,
trx_eu_units,
nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from default.tb_chnl_views_dif_agr_r_$$mktsplit
""")

tb_chnl_views_total2.registerTempTable("tb_chnl_views_total2")

tb_chnl_views_total1=spark.sql(""" 
select 
az_cust_id,
az_geo_id,
az_prod_id,
az_prod_ind,
az_mkt_id,
az_plan_id,
tb_id,
tb_cd,
tb_type,
src_sys_psbr_id,
AZ_TEAM_ID,
team_nm,
chnl_nm,
nrx,
nrx_units,
nrx_factrd_units,
nrx_eu_units,
trx,
trx_units,
trx_factrd_units,
trx_eu_units,
nw_brd_rx,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
ihc_ind,
kaiser_ind,
nosee_ind,
ama_no_contact_ind,
pueblo_mkt_sna_ind,
pueblo_mkt_exc_ind,
pueblo_mkt_blk_ind,
legal_ind,
aspen_ind,
dns_ind,
non_eval_ind,
encumbered_psbr_ind,
purple_excl_ind,
95_excl_ind,
az_brd_nm,
plan_nm,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
modl_var_typ,
frmy_tier,
frmy_sta,
frmy_posn,
medicaid_pln_blk,
tricare_pln_blk,
load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from default.tb_chnl_views_dif_rnr_all_$$mktsplit
""")

tb_chnl_views_total1.registerTempTable("tb_chnl_views_total1")

spark.sql("""Drop table if exists default.tb_chnl_views_total_$$mktsplit""")

tb_chnl_views_total_$$mktsplit=spark.sql("""
select * from tb_chnl_views_total1
union all
select * from tb_chnl_views_total2
""")

tb_chnl_views_total_$$mktsplit.write.mode("overwrite").saveAsTable("tb_chnl_views_total_$$mktsplit")

spark.sql("""refresh table default.tb_chnl_views_total_$$mktsplit""")
# CREATION OF A DATA FRAME FOR FILTERING TEAM BLOCKS FROM THE GENERIC LOOKUP AND CONFIGURATION TABLE
#spark.sql(""" drop table if exists CNFG_BLK_TEAM""")
CNFG_BLK_TEAM=spark.sql("""
SELECT TBL_NM as TBL_NM,PARM_TYPE as PARM_TYPE,VAL as VAL,ATTR1 as ATTR1
FROM cfg_gner_parm_val_tempdf
WHERE TBL_NM='temp_XPO_PLN_DYN_WKLY_BRD_FAM' and PARM_TYPE='BLK_TEAM' """)
CNFG_BLK_TEAM.registerTempTable("CNFG_BLK_TEAM")

#spark.sql("""analyze table CNFG_BLK_TEAM compute statistics""")

# CREATION OF A DATA FRAME FOR FILTERING MARKET BLOCKS FROM THE GENERIC LOOKUP AND CONFIGURATION TABLE
#spark.sql(""" drop table if exists CNFG_BLK_MKT""")
CNFG_BLK_MKT=spark.sql("""
SELECT TBL_NM,PARM_TYPE,VAL,ATTR1,ATTR2
FROM cfg_gner_parm_val_tempdf
WHERE TBL_NM='temp_XPO_PLN_DYN_WKLY_BRD_FAM' and PARM_TYPE='BLK_MKT' """)
CNFG_BLK_MKT.registerTempTable("CNFG_BLK_MKT")
#spark.sql("""analyze table CNFG_BLK_MKT compute statistics""")

D_PROD_FLTR=spark.sql("""SELECT DISTINCT
B.AZ_BRD_ID,
B.brd_fam_id ,
B.brd_fam_ds,
B.brd_fam_type,
B.mkt_vol_ind,
B.MKT_NM,
B.AZ_MKT_ID
from d_prod_tempdf B""")

D_PROD_FLTR.registerTempTable("D_PROD_FLTR")
#spark.sql("""cache table D_PROD_FLTR""")

cfg_cust_comm_blk=spark.sql("""
select /*+ broadcast(d_data_cald) */
trim(team_nm) team_nm,
trim(mkt_nm)  mkt_nm ,
trim(block_typ) block_typ,
trim(src_sys_nm) src_sys_nm,
trim(tog_val) tog_val,
trim(attr1) attr1,
trim(attr2) attr2,
trim(attr3) attr3,
trim(attr4) attr4,
trim(attr5) attr5,
trim(attr6) attr6,
trim(attr7) attr7,
trim(attr8) attr8,
trim(attr9) attr9,
trim(attr10) attr10
from cfg_cust_comm_blk_tempdf cfg
join d_data_cald_tempdf 
on rpt_wk between  eff_dt and end_dt
where lower(cfg.src_sys_nm)='retail' and upper(dsrc_nm)='SLS' and upper(appl_nm)='MAT'
 """)
 
cfg_cust_comm_blk.registerTempTable("cfg_cust_comm_blk") 

#spark.sql("""cache table cfg_cust_comm_blk""")

table_name=table_list.filter(table_list.tableName=="temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit""")
else:
    print("table not found")
    
temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit=spark.sql("""
SELECT /*+ broadcast(B) */
A.az_cust_id,
A.az_geo_id,
B.brd_fam_id ,
B.brd_fam_ds,
B.brd_fam_type,
A.az_prod_ind,
B.mkt_vol_ind,
B.MKT_NM,
A.az_mkt_id,
A.az_plan_id,
A.az_bot_acct_id,
A.bot_acct_nm,
A.tb_id,
A.tb_cd,
A.tb_type,
A.src_sys_psbr_id,
A.az_team_id,
A.team_nm,
A.chnl_nm,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
A.nw_brd_rx,
A.cust_clas,
A.cust_nm,
A.cust_typ,
A.cust_sta_cd,
A.pri_spec_cd,
A.mkt_spec_nm,
A.trgt_psbr_mkt_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.ihc_ind,
A.kaiser_ind,
A.nosee_ind,
A.ama_no_contact_ind,
MAX(A.pueblo_mkt_exc_ind) over(partition by az_cust_id,AZ_GEO_ID,tb_id,A.az_mkt_id ) AS pueblo_mkt_exc_ind ,
MAX(A.pueblo_mkt_sna_ind) over(partition by az_cust_id,AZ_GEO_ID,tb_id,A.az_mkt_id ) AS pueblo_mkt_sna_ind ,
MAX(A.pueblo_mkt_blk_ind) over(partition by az_cust_id,AZ_GEO_ID,tb_id,A.az_mkt_id ) AS pueblo_mkt_blk_ind ,
A.legal_ind,
A.aspen_ind,
MAX(A.dns_ind) over(partition by az_cust_id,AZ_GEO_ID,tb_id,A.az_mkt_id ) as dns_ind, 
A.non_eval_ind,
A.encumbered_psbr_ind,
A.medicaid_pln_blk,
MAX(A.purple_excl_ind) over(partition by az_cust_id,AZ_GEO_ID,tb_id,A.az_mkt_id ) AS purple_excl_ind ,
A.95_excl_ind,
A.tricare_pln_blk,
A.plan_nm,
A.pay_typ,
A.modl_var_typ,
A.frmy_tier,
A.frmy_sta,
A.frmy_posn,
A.load_dt
FROM
default.tb_chnl_views_total_$$mktsplit A
JOIN
D_PROD_FLTR B
ON A.az_mkt_id =B.az_mkt_id
and
A.az_prod_id=B.AZ_BRD_ID
""")

temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit")
spark.sql("""refresh table default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit""")
# spark.sql DRG changes
import pyspark.sql.functions as f
from pyspark.sql.functions import first
from pyspark.sql.functions import col, lit, min, row_number
from pyspark.sql.functions import split

cfg_cust_comm_blk_vertical = spark.sql(""" 
select 
a.*,trim(b.block_typ) as block_typ,
trim(b.src_sys_nm) src_sys_nm,
trim(b.tog_val) tog_val,
trim(b.attr1) attr1,
trim(attr2) attr2,
trim(attr3) attr3,
trim(attr4) attr4,
trim(attr5) attr5,
trim(attr6) attr6,
trim(attr7) attr7,
trim(attr8) attr8,
trim(attr9) attr9,
trim(attr10) attr10 from(select
distinct
trim(cfg.team_nm) team_nm,
trim(cfg.mkt_nm)  mkt_nm ,
max(case when trim(cfg.block_typ)='ihc_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS ihc_ind,
max(case when trim(cfg.block_typ)='dnr_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS dnr_ind,
max(case when trim(cfg.block_typ)='kaiser_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS kaiser_ind,
max(case when trim(cfg.block_typ)='legal_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS legal_ind,
max(case when trim(cfg.block_typ)='non_eval_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS non_eval_ind,
max(case when trim(cfg.block_typ)='nosee_ind' then 'Y' ELSE 'N' END) AS nosee_ind,
max(case when trim(cfg.block_typ)='pueblo_mkt_exc_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS pueblo_mkt_exc_ind,
max(case when trim(cfg.block_typ)='pueblo_mkt_blk_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS pueblo_mkt_blk_ind,
max(case when trim(cfg.block_typ)='pueblo_mkt_sna_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS pueblo_mkt_sna_ind,
max(case when trim(cfg.block_typ)='purple_excl_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS purple_excl_ind,
max(case when trim(cfg.block_typ)='medicaid_pln_blk'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS medicaid_pln_blk,
max(case when trim(cfg.block_typ)='aspen_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS aspen_ind,
max(case when trim(cfg.block_typ)='dns_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS dns_ind,
max(case when trim(cfg.block_typ)='encumbered_psbr_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS encumbered_psbr_ind,
max(case when trim(cfg.block_typ)='95_excl_ind'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS 95_excl_ind,
max(case when trim(cfg.block_typ)='tricare_pln_blk'  and trim(tog_val)='Y' then 'Y' ELSE 'N' END) AS tricare_pln_blk
from cfg_cust_comm_blk cfg group by trim(cfg.team_nm),trim(cfg.mkt_nm) )a join cfg_cust_comm_blk b on a.mkt_nm=b.mkt_nm and a.team_nm=b.team_nm
""")

cfg_cust_comm_blk_vertical.registerTempTable("cfg_cust_comm_blk_vertical")

cfg_cust_comm_blk_veritical_com_attr1 = cfg_cust_comm_blk_vertical.groupby("mkt_nm", "team_nm", "src_sys_nm", "tog_val",
                                                                           "ihc_ind", "dnr_ind", "kaiser_ind",
                                                                           "legal_ind", "non_eval_ind", "nosee_ind",
                                                                           "pueblo_mkt_exc_ind", "pueblo_mkt_blk_ind",
                                                                           "pueblo_mkt_sna_ind", "purple_excl_ind",
                                                                           "medicaid_pln_blk", "aspen_ind", "dns_ind",
                                                                           "encumbered_psbr_ind", "95_excl_ind",
                                                                           "tricare_pln_blk", "attr2", "attr3", "attr4",
                                                                           "attr5", "attr6", "attr7", "attr8", "attr9",
                                                                           "attr10", "block_typ").agg(
    f.concat_ws(",", f.collect_list(cfg_cust_comm_blk_vertical.attr1)).alias("attr1"))

cfg_cust_comm_blk_veritical_com_attr1.registerTempTable("cfg_cust_comm_blk_veritical_com_attr1")

cfg_cust_comm_blk_horizontal = cfg_cust_comm_blk_veritical_com_attr1.groupBy("mkt_nm", "team_nm", "src_sys_nm",
                                                                             "ihc_ind", "dnr_ind", "kaiser_ind",
                                                                             "legal_ind", "non_eval_ind", "nosee_ind",
                                                                             "pueblo_mkt_exc_ind", "pueblo_mkt_blk_ind",
                                                                             "pueblo_mkt_sna_ind", "purple_excl_ind",
                                                                             "medicaid_pln_blk", "aspen_ind", "dns_ind",
                                                                             "encumbered_psbr_ind", "95_excl_ind",
                                                                             "tricare_pln_blk", "attr2", "attr3",
                                                                             "attr4", "attr5", "attr6", "attr7",
                                                                             "attr8", "attr9", "attr10").pivot(
    "block_typ").agg(
    first("attr1").alias("attr1"),
    first("tog_val").alias("tog_val"))

cfg_cust_comm_blk_horizontal.createOrReplaceTempView("cfg_cust_comm_blk_horizontal")

cfg_cust_comm_blk_horizontal_attr_split = cfg_cust_comm_blk_horizontal.withColumn("dnr_ind_attr1_col1", split(col("dnr_ind_attr1"), "\\,").getItem(0)).withColumn("dnr_ind_attr1_col2", split(col("dnr_ind_attr1"), "\\,").getItem(1))

cfg_cust_comm_blk_horizontal_attr_split.createOrReplaceTempView("cfg_cust_comm_blk_horizontal_attr_split")

cfg_cust_comm_blk_horizontal_attr_split_tbl=spark.sql("""select * from cfg_cust_comm_blk_horizontal_attr_split""")
cfg_cust_comm_blk_horizontal_attr_split_tbl.registerTempTable("cfg_cust_comm_blk_horizontal_attr_split_tbl")

'''
spark.sql("""
CREATE  TABLE temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk1 STORED AS PARQUET TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS
SELECT /*+ broadcast(cfg) */
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.az_prod_ind,
A.mkt_vol_ind,
A.MKT_NM,
A.az_mkt_id,
A.az_plan_id,
A.az_bot_acct_id,
A.bot_acct_nm,
A.tb_id,
A.tb_cd,
A.tb_type,
A.src_sys_psbr_id,
A.az_team_id,
A.team_nm,
A.chnl_nm,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
A.nw_brd_rx,
A.cust_clas,
A.cust_nm,
A.cust_typ,
A.cust_sta_cd,
A.pri_spec_cd,
A.mkt_spec_nm,
A.trgt_psbr_mkt_ind,
A.pdrp_opt_sta,
A.ama_sta,
Max(CASE WHEN cfg.BLOCK_TYP IN('ihc_ind') AND cfg.TOG_VAL='Y' THEN A.ihc_ind ELSE 'N' END) ihc_ind,
Max(CASE WHEN cfg.BLOCK_TYP IN('kaiser_ind') AND cfg.TOG_VAL='Y' THEN A.kaiser_ind ELSE 'N' END) kaiser_ind,
Max(CASE WHEN cfg.BLOCK_TYP IN('nosee_ind') AND lower(a.nosee_ind)=lower(cfg.attr1) THEN 'Y' ELSE 'N' END) nosee_ind,
Max(CASE WHEN cfg.BLOCK_TYP IN('pueblo_mkt_exc_ind') AND cfg.TOG_VAL='Y' THEN A.pueblo_mkt_exc_ind ELSE 'N' END) pueblo_mkt_exc_ind,
Max(CASE WHEN cfg.BLOCK_TYP IN('pueblo_mkt_sna_ind') AND cfg.TOG_VAL='Y' THEN A.pueblo_mkt_sna_ind ELSE 'N' END) pueblo_mkt_sna_ind,
Max(CASE WHEN cfg.BLOCK_TYP IN('pueblo_mkt_blk_ind') AND cfg.TOG_VAL='Y' THEN A.pueblo_mkt_blk_ind ELSE 'N' END) pueblo_mkt_blk_ind,
Max(CASE WHEN cfg.BLOCK_TYP IN('legal_ind') AND cfg.TOG_VAL='Y' THEN A.legal_ind ELSE 'N' END) legal_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('purple_excl_ind') AND cfg.TOG_VAL='Y' and UPPER(A.cust_clas)='OTH' THEN A.purple_excl_ind ELSE 'N' END) purple_excl_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('non_eval_ind') AND cfg.TOG_VAL='Y' and UPPER(A.cust_clas)='HCP' THEN A.non_eval_ind ELSE 'N' END) non_eval_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('medicaid_pln_blk') AND cfg.TOG_VAL='Y' THEN A.medicaid_pln_blk ELSE 'N' END) medicaid_pln_blk,
max(CASE WHEN cfg.BLOCK_TYP IN('aspen_ind') AND cfg.TOG_VAL='Y' THEN A.aspen_ind ELSE 'N' END) aspen_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('dns_ind') AND cfg.TOG_VAL='Y' THEN A.dns_ind ELSE 'N' END) dns_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('encumbered_psbr_ind') AND cfg.TOG_VAL='Y' THEN A.encumbered_psbr_ind ELSE 'N' END) encumbered_psbr_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('95_excl_ind') AND cfg.TOG_VAL='Y' THEN A.95_excl_ind ELSE 'N' END) 95_excl_ind,
max(CASE WHEN cfg.BLOCK_TYP IN('tricare_pln_blk') AND cfg.TOG_VAL='Y' THEN A.tricare_pln_blk ELSE 'N' END) tricare_pln_blk,
max(CASE WHEN cfg.BLOCK_TYP IN('dnr_ind') AND cfg.TOG_VAL='Y' and upper(A.cust_sta_cd)=upper(cfg.attr1) THEN 'Y' ELSE 'N' END) dnr_ind,
A.plan_nm,
A.pay_typ,
A.modl_var_typ,
A.frmy_tier,
A.frmy_sta,
A.frmy_posn,
A.load_dt
FROM
default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit A
left join
cfg_cust_comm_blk cfg
on
A.team_nm=cfg.team_nm
and A.mkt_nm=cfg.mkt_nm
group by
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.az_prod_ind,
A.mkt_vol_ind,
A.MKT_NM,
A.az_mkt_id,
A.az_plan_id,
A.az_bot_acct_id,
A.bot_acct_nm,
A.tb_id,
A.tb_cd,
A.tb_type,
A.src_sys_psbr_id,
A.az_team_id,
A.team_nm,
A.chnl_nm,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
A.nw_brd_rx,
A.cust_clas,
A.cust_nm,
A.cust_typ,
A.cust_sta_cd,
A.pri_spec_cd,
A.mkt_spec_nm,
A.trgt_psbr_mkt_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.plan_nm,
A.pay_typ,
A.modl_var_typ,
A.frmy_tier,
A.frmy_sta,
A.frmy_posn,
A.load_dt
""")
'''

temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk1=spark.sql("""
SELECT /*+ broadcast(cfg) */
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.az_prod_ind,
A.mkt_vol_ind,
A.MKT_NM,
A.chnl_nm,
A.az_mkt_id,
A.az_plan_id,
A.az_bot_acct_id,
A.bot_acct_nm,
A.tb_id,
A.tb_cd,
A.tb_type,
A.src_sys_psbr_id,
A.az_team_id,
A.team_nm,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
A.nw_brd_rx,
A.cust_clas,
A.cust_nm,
A.cust_typ,
A.cust_sta_cd,
A.pri_spec_cd,
A.mkt_spec_nm,
A.trgt_psbr_mkt_ind,
A.pdrp_opt_sta,
A.ama_sta,
CASE WHEN cfg.ihc_ind='Y' THEN A.ihc_ind ELSE 'N' END ihc_ind,
CASE WHEN cfg.kaiser_ind='Y' THEN A.kaiser_ind ELSE 'N' END kaiser_ind,
case when cfg.nosee_ind='Y' and (lower(a.nosee_ind)=lower(cfg.nosee_ind_attr1)) THEN 'Y' ELSE 'N' END AS nosee_ind,
CASE WHEN cfg.pueblo_mkt_exc_ind='Y' THEN A.pueblo_mkt_exc_ind ELSE 'N' END pueblo_mkt_exc_ind,
CASE WHEN cfg.pueblo_mkt_sna_ind='Y' THEN A.pueblo_mkt_sna_ind ELSE 'N' END pueblo_mkt_sna_ind,
CASE WHEN cfg.pueblo_mkt_blk_ind='Y' THEN A.pueblo_mkt_blk_ind ELSE 'N' END pueblo_mkt_blk_ind,
CASE WHEN cfg.legal_ind='Y' THEN A.legal_ind ELSE 'N' END legal_ind,
CASE WHEN cfg.purple_excl_ind='Y' and UPPER(A.cust_clas)='OTH' THEN A.purple_excl_ind ELSE 'N' END purple_excl_ind,
CASE WHEN cfg.non_eval_ind='Y' and UPPER(A.cust_clas)='HCP' THEN A.non_eval_ind ELSE 'N' END non_eval_ind,
CASE WHEN cfg.medicaid_pln_blk='Y' THEN A.medicaid_pln_blk ELSE 'N' END medicaid_pln_blk,
CASE WHEN cfg.aspen_ind='Y' THEN A.aspen_ind ELSE 'N' END aspen_ind,
CASE WHEN cfg.dns_ind='Y' THEN A.dns_ind ELSE 'N' END dns_ind,
CASE WHEN cfg.encumbered_psbr_ind='Y' THEN A.encumbered_psbr_ind ELSE 'N' END encumbered_psbr_ind,
CASE WHEN cfg.95_excl_ind='Y' THEN A.95_excl_ind ELSE 'N' END 95_excl_ind,
CASE WHEN cfg.tricare_pln_blk='Y' THEN A.tricare_pln_blk ELSE 'N' END tricare_pln_blk,
CASE when cfg.dnr_ind='Y' and (upper(a.cust_sta_cd)=upper(cast(cfg.dnr_ind_attr1_col1 as string)) or upper(a.cust_sta_cd)=upper(cast(cfg.dnr_ind_attr1_col2 as string)) or upper(a.cust_sta_cd)=upper(cast(cfg.dnr_ind_attr1 as string))) THEN 'Y' ELSE 'N' END AS dnr_ind,
A.plan_nm,
A.pay_typ,
A.modl_var_typ,
A.frmy_tier,
A.frmy_sta,
A.frmy_posn,
A.load_dt
FROM
default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit A
left join cfg_cust_comm_blk_horizontal_attr_split_tbl cfg
on A.mkt_nm=cfg.mkt_nm and A.team_nm=cfg.team_nm
""")

temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk1.registerTempTable("temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk1")


#APPLICATION OF BLOCKING ON WEEKLY AGGREGATE BASED ON TEAM AND MARKET PARAMETERS FOR IHC,KAISER,MEDICAID AND TRICARE BLOCKS
#spark.sql(""" drop table if exists temp_XPO_PLN_DYN_WKLY_BRD_FAM""")
temp_XPO_PLN_DYN_WKLY_BRD_FAM=spark.sql("""
SELECT 
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.az_prod_ind,
A.mkt_vol_ind,
A.MKT_NM,
A.az_mkt_id,
A.az_plan_id,
A.tb_id,
A.tb_cd,
A.tb_type,
A.src_sys_psbr_id,
A.az_team_id,
A.team_nm,
A.chnl_nm,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
A.nw_brd_rx,
A.cust_clas,
A.cust_nm,
A.cust_typ,
A.cust_sta_cd,
A.pri_spec_cd,
A.mkt_spec_nm,
A.trgt_psbr_mkt_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.plan_nm,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.modl_var_typ,
A.frmy_tier,
A.frmy_sta,
A.frmy_posn,
A.load_dt,
A.non_eval_ind
FROM
temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk1 A
WHERE
upper(A.ihc_ind) ='N' and
upper(A.kaiser_ind)='N' and
upper(A.nosee_ind)='N' and
upper(A.pueblo_mkt_exc_ind)='N' and
upper(A.pueblo_mkt_sna_ind)='N' and
upper(A.legal_ind)='N' and
upper(A.purple_excl_ind)='N' and
upper(A.non_eval_ind) = 'N' and
upper(A.medicaid_pln_blk)='N' and
upper(pueblo_mkt_blk_ind)='N'
and upper(aspen_ind)='N'
and upper(dns_ind)='N'
and upper(encumbered_psbr_ind)='N'
and upper(95_excl_ind)='N'
and upper(tricare_pln_blk)='N'
and upper(dnr_ind)='N'
""")
temp_XPO_PLN_DYN_WKLY_BRD_FAM.createOrReplaceTempView("temp_XPO_PLN_DYN_WKLY_BRD_FAM")

temp_cust_1=spark.sql("""select distinct cust_nm,az_cust_id from d_cust_tempdf""")
temp_cust_1.createOrReplaceTempView("temp_cust_1")

#temp_adhoc_blk_cust_id=spark.sql("""

temp_adhoc_blk_cust_id=spark.sql("""
select /*+ broadcast(cust) */
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
MAX(CASE WHEN BLOCK_TYP IN('HCP_AZ_ADHOC') AND cust.az_cust_id LIKE CONCAT('%',ATTR1,'%')  THEN 'Y' ELSE 'N' END) as HCP_AZ_ADHOC ,
'N' as HCP_NM_ADHOC,
'N' as HCP_PSID_ADHOC,
'N' as HCP_PSNM_ADHOC
from
cfg_cust_comm_blk cfg
join
temp_cust_1 cust
on
cfg.attr1=cust.az_cust_id
group by 
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
HCP_NM_ADHOC,
HCP_PSID_ADHOC,
HCP_PSNM_ADHOC
""")

temp_adhoc_blk_cust_id.createOrReplaceTempView("temp_adhoc_blk_cust_id")

#temp_adhoc_blk_cust_name=spark.sql("""

temp_adhoc_blk_cust_name=spark.sql("""
select  /*+ broadcast(cust) */
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
'N' as HCP_AZ_ADHOC ,
MAX(CASE WHEN BLOCK_TYP IN('HCP_NM_ADHOC') AND cust.cust_nm LIKE CONCAT('%',ATTR2,'%')  THEN 'Y' ELSE 'N' END) as HCP_NM_ADHOC,
'N' as HCP_PSID_ADHOC,
'N' as HCP_PSNM_ADHOC
from
cfg_cust_comm_blk cfg
join
temp_cust_1 cust
on
cfg.attr2=cust.cust_nm
group by 
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
HCP_AZ_ADHOC,
HCP_PSID_ADHOC,
HCP_PSNM_ADHOC
""")

temp_adhoc_blk_cust_name.createOrReplaceTempView("temp_adhoc_blk_cust_name")

temp_cust_2=spark.sql("""select distinct cust_nm,pri_spec_cd, az_cust_id from d_cust_tempdf""")
temp_cust_2.createOrReplaceTempView("temp_cust_2")

#temp_adhoc_blk_cust_prispec_id=spark.sql("""

temp_adhoc_blk_cust_prispec_id=spark.sql("""
select  /*+ broadcast(cust) */
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
'N' as HCP_AZ_ADHOC ,
'N' as HCP_NM_ADHOC,
MAX(CASE WHEN BLOCK_TYP IN('HCP_PSID_ADHOC') AND cust.pri_spec_cd LIKE CONCAT('%',ATTR3,'%')  THEN 'Y' ELSE 'N' END) as HCP_PSID_ADHOC,
'N' as HCP_PSNM_ADHOC
from
cfg_cust_comm_blk cfg
join
temp_cust_2 cust
on
cfg.attr3=cust.pri_spec_cd
group by 
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
HCP_AZ_ADHOC,
HCP_NM_ADHOC,
HCP_PSNM_ADHOC
""")

temp_adhoc_blk_cust_prispec_id.createOrReplaceTempView("temp_adhoc_blk_cust_prispec_id")

#temp_adhoc_blk_cust_prispec_desc=spark.sql("""

temp_cust_3=spark.sql("""select distinct cust_nm,pri_spec_desc, az_cust_id from d_cust_tempdf""")
temp_cust_3.createOrReplaceTempView("temp_cust_3")

temp_adhoc_blk_cust_prispec_desc=spark.sql("""
select  /*+ broadcast(cust) */
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
'N' as HCP_AZ_ADHOC ,
'N' as HCP_NM_ADHOC,
'N' as HCP_PSID_ADHOC,
MAX(CASE WHEN BLOCK_TYP IN('HCP_PSNM_ADHOC') AND cust.pri_spec_desc LIKE CONCAT('%',ATTR4,'%') THEN 'Y' ELSE 'N' END) as HCP_PSNM_ADHOC
from
cfg_cust_comm_blk cfg
join
temp_cust_3 cust
on
cfg.attr4=cust.pri_spec_desc
group by 
cfg.team_nm,
cfg.mkt_nm,
cust.az_cust_id,
HCP_AZ_ADHOC,
HCP_NM_ADHOC,
HCP_PSID_ADHOC
""")

temp_adhoc_blk_cust_prispec_desc.createOrReplaceTempView("temp_adhoc_blk_cust_prispec_desc")

#merging all blocking
#temp_blk_adhoc=

#spark.sql(""" drop table if exists temp_blk_adhoc""")

temp_blk_adhoc=spark.sql("""
select C.team_nm,C.mkt_nm,C.az_cust_id,C.HCP_AZ_ADHOC ,C.HCP_NM_ADHOC,C.HCP_PSID_ADHOC,C.HCP_PSNM_ADHOC from temp_adhoc_blk_cust_prispec_id C
union
select D.team_nm,D.mkt_nm,D.az_cust_id,D.HCP_AZ_ADHOC ,D.HCP_NM_ADHOC,D.HCP_PSID_ADHOC,D.HCP_PSNM_ADHOC from temp_adhoc_blk_cust_prispec_desc D
union 
select A.team_nm,A.mkt_nm,A.az_cust_id,A.HCP_AZ_ADHOC ,A.HCP_NM_ADHOC,A.HCP_PSID_ADHOC,A.HCP_PSNM_ADHOC from temp_adhoc_blk_cust_id A
union
select B.team_nm,B.mkt_nm,B.az_cust_id,B.HCP_AZ_ADHOC ,B.HCP_NM_ADHOC,B.HCP_PSID_ADHOC,B.HCP_PSNM_ADHOC from temp_adhoc_blk_cust_name B
""")

#spark.sql("""
#insert into TABLE temp_blk_adhoc
#select A.team_nm,A.mkt_nm,A.az_cust_id,A.HCP_AZ_ADHOC ,A.HCP_NM_ADHOC,A.HCP_PSID_ADHOC,A.HCP_PSNM_ADHOC from temp_adhoc_blk_cust_id A
#""")
#
#spark.sql("""
#insert into TABLE temp_blk_adhoc
#select B.team_nm,B.mkt_nm,B.az_cust_id,B.HCP_AZ_ADHOC ,B.HCP_NM_ADHOC,B.HCP_PSID_ADHOC,B.HCP_PSNM_ADHOC from temp_adhoc_blk_cust_name B
#""")

temp_blk_adhoc.createOrReplaceTempView("temp_blk_adhoc")

#deduping on grain 
temp_blk_cust_adhoc=spark.sql("""
select A.team_nm,A.mkt_nm,A.az_cust_id,
max(A.HCP_AZ_ADHOC) HCP_AZ_ADHOC ,max(HCP_NM_ADHOC)HCP_NM_ADHOC ,max(HCP_PSID_ADHOC)HCP_PSID_ADHOC,max(HCP_PSNM_ADHOC)HCP_PSNM_ADHOC 
from temp_blk_adhoc A
group by A.team_nm,A.mkt_nm,A.az_cust_id
""")
temp_blk_cust_adhoc.createOrReplaceTempView("temp_blk_cust_adhoc")

temp_plan_blk1=spark.sql("""
SELECT distinct 
A.az_team_id,
A.team_nm,
A.az_mkt_id,
A.MKT_NM,
A.az_cust_id,
A.az_plan_id,
A.plan_nm,
A.az_bot_acct_id,
A.bot_acct_nm,
A.chnl_nm
FROM
temp_XPO_PLN_DYN_WKLY_BRD_FAM A""")

temp_plan_blk1.registerTempTable("temp_plan_blk1")

temp_plan_adhoc=spark.sql(""" 
SELECT   /*+ broadcast(cfg) */
A.az_team_id,
A.team_nm,
A.az_mkt_id,
A.MKT_NM,
A.az_cust_id,
A.az_plan_id,
A.plan_nm,
A.az_bot_acct_id,
A.bot_acct_nm,
A.chnl_nm,
MAX(CASE WHEN BLOCK_TYP IN('PLAN_AZ_ADHOC')  AND A.az_plan_id LIKE CONCAT('%',ATTR1,'%') THEN 'Y' ELSE 'N' END) as PLAN_AZ_ADHOC,
MAX(CASE WHEN BLOCK_TYP IN('PLAN_NM_ADHOC')  AND A.plan_nm LIKE CONCAT('%',ATTR1,'%')  THEN 'Y' ELSE 'N' END) as PLAN_NM_ADHOC, 
MAX(CASE WHEN BLOCK_TYP IN('BOT_AZ_ADHOC') AND A.az_bot_acct_id LIKE CONCAT('%',ATTR1,'%')  THEN 'Y' ELSE 'N' END) as BOT_AZ_ADHOC,
MAX(CASE WHEN BLOCK_TYP IN('BOT_NM_ADHOC') AND A.bot_acct_nm LIKE CONCAT('%',ATTR1,'%')  THEN 'Y' ELSE 'N' END) as BOT_NM_ADHOC
from 
temp_plan_blk1 a
join 
cfg_cust_comm_blk cfg
on a.team_nm=cfg.team_nm
and a.MKT_NM=cfg.MKT_NM
group by 
A.az_team_id,
A.team_nm,
A.az_mkt_id,
A.MKT_NM,
A.az_cust_id,
A.az_plan_id,
A.plan_nm,
A.az_bot_acct_id,
A.bot_acct_nm,
A.chnl_nm
""")
temp_plan_adhoc.createOrReplaceTempView("temp_plan_adhoc")

#apply adhoc blocking 

temp_XPO_PLN_DYN_WKLY_BRD_FAM_blk1=spark.sql("""
SELECT /*+ broadcast(blk1) */
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.az_prod_ind,
A.mkt_vol_ind,
A.MKT_NM,
A.az_mkt_id,
A.az_plan_id,
A.tb_id,
A.tb_cd,
A.tb_type,
A.src_sys_psbr_id,
A.az_team_id,
A.team_nm,
A.chnl_nm,
A.nrx,
A.nrx_units,
A.nrx_factrd_units,
A.nrx_eu_units,
A.trx,
A.trx_units,
A.trx_factrd_units,
A.trx_eu_units,
A.nw_brd_rx,
A.cust_clas,
A.cust_nm,
A.cust_typ,
A.cust_sta_cd,
A.pri_spec_cd,
A.mkt_spec_nm,
A.trgt_psbr_mkt_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.plan_nm,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.modl_var_typ,
A.frmy_tier,
A.frmy_sta,
A.frmy_posn,
A.load_dt,
A.non_eval_ind,
nvl(HCP_AZ_ADHOC,'N')HCP_AZ_ADHOC, 
nvl(HCP_NM_ADHOC,'N')HCP_NM_ADHOC, 
nvl(HCP_PSID_ADHOC,'N')HCP_PSID_ADHOC, 
nvl(HCP_PSNM_ADHOC,'N')HCP_PSNM_ADHOC, 
nvl(PLAN_AZ_ADHOC,'N')PLAN_AZ_ADHOC, 
nvl(PLAN_NM_ADHOC,'N')PLAN_NM_ADHOC, 
nvl(BOT_AZ_ADHOC,'N')BOT_AZ_ADHOC, 
nvl(BOT_NM_ADHOC,'N')BOT_NM_ADHOC 
FROM
temp_XPO_PLN_DYN_WKLY_BRD_FAM A
left join
temp_blk_cust_adhoc blk1
on A.az_cust_id=blk1.az_cust_id
and A.team_nm=blk1.team_nm
and A.mkt_nm=blk1.mkt_nm
left join temp_plan_adhoc blk2
on a.az_team_id=blk2.az_team_id
and a.az_mkt_id=blk2.az_mkt_id
and a.az_cust_id=blk2.az_cust_id
and a.az_plan_id=blk2.az_plan_id
and a.az_bot_acct_id=blk2.az_bot_acct_id
and a.chnl_nm=blk2.chnl_nm
""")

temp_XPO_PLN_DYN_WKLY_BRD_FAM_blk1.registerTempTable("temp_XPO_PLN_DYN_WKLY_BRD_FAM_blk1")

#AGGREGATING THE WEEKLY XPO AND PLN_DYN TABLE AT CUST,GEO,BRD_FAM,MKT,PLAN,TEAM,PAYTYPE AND TRGT_PSBR_BRD_IND
table_name=table_list.filter(table_list.tableName=="temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit""")
else:
    print("table not found")
    
temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit=spark.sql("""
SELECT
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
MAX(A.az_prod_ind) as az_prod_ind,
A.mkt_vol_ind,
A.az_mkt_id,
A.az_plan_id,
MIN(tb_id) as tb_id,
A.tb_cd,
A.src_sys_psbr_id,
A.az_team_id,
MAX(trgt_psbr_mkt_ind) as trgt_psbr_mkt_ind ,
MAX(A.pdrp_opt_sta) as pdrp_opt_sta,
MAX(A.ama_sta) as ama_sta,
A.pay_typ,
A.az_bot_acct_id,
A.chnl_nm,
MAX(bot_acct_nm) as bot_acct_nm,
SUM(CASE WHEN tb_type='CURR' THEN A.nrx ELSE 0 END ) AS CUR_NRX_CT ,
SUM(CASE WHEN tb_type='PREV' THEN A.nrx ELSE 0 END ) AS PREV_NRX_CT ,
SUM(CASE WHEN tb_type='CURR' THEN A.nrx_units ELSE 0 END ) AS CUR_nrx_ut ,
SUM(CASE WHEN tb_type='PREV' THEN A.nrx_units ELSE 0 END ) AS PREV_NRX_ut ,
SUM(CASE WHEN tb_type='CURR' THEN A.nrx_factrd_units ELSE 0 END ) AS CUR_nrx_factrd_ut,
SUM(CASE WHEN tb_type='PREV' THEN A.nrx_factrd_units ELSE 0 END ) AS PREV_nrx_factrd_ut,
SUM(CASE WHEN tb_type='CURR' THEN A.trx ELSE 0 END ) AS CUR_TRX_CT ,
SUM(CASE WHEN tb_type='PREV' THEN A.trx ELSE 0 END ) AS PREV_TRX_CT ,
SUM(CASE WHEN tb_type='CURR' THEN A.trx_units ELSE 0 END ) AS CUR_Trx_ut ,
SUM(CASE WHEN tb_type='PREV' THEN A.trx_units ELSE 0 END ) AS PREV_TRX_ut ,
SUM(CASE WHEN tb_type='CURR' THEN A.trx_factrd_units ELSE 0 END ) AS CUR_Trx_factrd_ut,
SUM(CASE WHEN tb_type='PREV' THEN A.trx_factrd_units ELSE 0 END ) AS PREV_Trx_factrd_ut,
SUM(CASE WHEN tb_type='CURR' THEN A.nw_brd_rx ELSE 0 END ) AS CUR_nbrx_CT ,
SUM(CASE WHEN tb_type='PREV' THEN A.nw_brd_rx ELSE 0 END ) AS PREV_nbrx_CT ,
SUM(CASE WHEN tb_type='CURR' THEN  A.nrx_eu_units ELSE 0 END ) AS CUR_NRX_EU ,
SUM(CASE WHEN tb_type='PREV' THEN  A.nrx_eu_units ELSE 0 END ) AS PREV_NRX_EU ,
SUM(CASE WHEN tb_type='CURR' THEN  A.trx_eu_units ELSE 0 END ) AS CUR_TRX_EU ,
SUM(CASE WHEN tb_type='PREV' THEN  A.trx_eu_units ELSE 0 END ) AS PREV_TRX_EU ,
MAX(A.brd_fam_ds) as brd_fam_ds,
MAX(A.BRD_FAM_TYPE) as BRD_FAM_TYPE,
MAX(A.MKT_NM) as MKT_NM,
MAX(team_nm) as team_nm,
MAX(A.cust_clas) as cust_clas,
MAX(A.cust_nm) as cust_nm,
MAX(A.cust_typ) as cust_typ,
MAX(A.cust_sta_cd) as cust_sta_cd,
MAX(A.pri_spec_cd) as pri_spec_cd,
MAX(A.mkt_spec_nm) as mkt_spec_nm,
MAX(A.plan_nm) as plan_nm,
MAX(A.frmy_tier) as frmy_tier,
MAX(A.frmy_sta) as frmy_sta,
MAX(A.frmy_posn) as frmy_posn,
MAX(A.load_dt) as load_dt
FROM
temp_XPO_PLN_DYN_WKLY_BRD_FAM_blk1 A
where 
upper(HCP_AZ_ADHOC)='N' and
upper(HCP_NM_ADHOC)='N' and
upper(HCP_PSID_ADHOC)='N' and
upper(HCP_PSNM_ADHOC)='N' and
upper(PLAN_AZ_ADHOC)='N' and
upper(PLAN_NM_ADHOC)='N' and
upper(BOT_AZ_ADHOC)='N' and
upper(BOT_NM_ADHOC)='N'
GROUP BY
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.mkt_vol_ind,
A.az_mkt_id,
A.az_plan_id,
A.tb_cd,
A.src_sys_psbr_id,
A.az_team_id,
A.pay_typ,
A.az_bot_acct_id,
A.chnl_nm
""")
temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit")
spark.sql("""refresh table default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit""")
#CREATION OF MARKET LEVEL METRICS BY PERFORMING ROW LEVEL OPERATION FOR WEEKLY AGGREGATED TABLE
table_name=table_list.filter(table_list.tableName=="temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit""")
else:
    print("table not found")
    
temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit=spark.sql("""
SELECT
A.az_cust_id,
A.az_geo_id,
A.brd_fam_id ,
A.AZ_PROD_IND,
A.mkt_vol_ind,
A.az_mkt_id,
A.az_plan_id,
A.tb_id,
A.tb_cd,
A.src_sys_psbr_id,
A.az_team_id,
MAX(A.trgt_psbr_mkt_ind) over(partition by az_cust_id,AZ_GEO_ID,az_mkt_id )  AS trgt_psbr_mkt_ind ,
A.pdrp_opt_sta,
A.ama_sta,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.chnl_nm,
CUR_NRX_CT ,
PREV_NRX_CT ,
CUR_nrx_ut ,
A.PREV_NRX_ut ,
A.CUR_nrx_factrd_ut,
A.PREV_nrx_factrd_ut,
A.CUR_TRX_CT ,
A.PREV_TRX_CT ,
A.CUR_Trx_ut ,
A.PREV_TRX_ut ,
A.CUR_Trx_factrd_ut,
A.PREV_Trx_factrd_ut,
A.CUR_nbrx_CT ,
A.PREV_nbrx_CT ,
A.CUR_NRX_EU ,
A.PREV_NRX_EU ,
A.CUR_TRX_EU ,
A.PREV_TRX_EU ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_NRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS cur_mkt_nrx_ct ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_NRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_nrx_ct,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_nrx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS CUR_mkt_nrx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_nrx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_nrx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_nrx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS CUR_mkt_nrx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_nrx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm) AS PREV_mkt_nrx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_TRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS cur_mkt_Trx_ct ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_TRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_Trx_ct,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_Trx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS CUR_mkt_Trx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_Trx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_Trx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_Trx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm) AS CUR_mkt_Trx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_Trx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_Trx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_NBRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm) AS cur_mkt_nBrx_ct ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_NBRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_nBrx_ct,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_NRX_Eu ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS cur_mkt_nrx_EU ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_NRX_EU ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS PREV_mkt_nrx_EU,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_TRX_Eu ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm ) AS cur_mkt_Trx_EU ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_TRX_EU ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm) AS PREV_mkt_Trx_EU,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_NRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS cur_bot_mkt_nrx_ct ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_NRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS PREV_bot_mkt_nrx_ct,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_nrx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS CUR_bot_mkt_nrx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_nrx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS PREV_bot_mkt_nrx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_nrx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS CUR_bot_mkt_nrx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_nrx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ) AS PREV_bot_mkt_nrx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_TRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS cur_bot_mkt_Trx_ct ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_TRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS PREV_bot_mkt_Trx_ct,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_Trx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS CUR_bot_mkt_Trx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_Trx_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ) AS PREV_bot_mkt_Trx_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_Trx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ) AS CUR_bot_mkt_Trx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_Trx_factrd_ut ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS PREV_bot_mkt_Trx_factrd_ut ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_NBRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ) AS cur_bot_mkt_nBrx_ct ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_NBRX_CT ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS PREV_bot_mkt_nBrx_ct,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_NRX_Eu ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS cur_bot_mkt_nrx_EU ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_NRX_EU ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS PREV_bot_mkt_nrx_EU,
SUM(CASE WHEN mkt_vol_ind='Y' THEN CUR_TRX_Eu ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ ) AS cur_bot_mkt_Trx_EU ,
SUM(CASE WHEN mkt_vol_ind='Y' THEN PREV_TRX_EU ELSE 0 END) over(partition by az_cust_id,AZ_GEO_ID,tb_id,az_mkt_id,chnl_nm,az_bot_acct_id,A.pay_typ) AS PREV_bot_mkt_Trx_EU,
brd_fam_ds,
brd_fam_type,
MKT_NM,
team_nm,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
plan_nm,
frmy_tier,
frmy_sta,
frmy_posn,
load_dt
FROM
default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit A
""")

temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit.write.mode("overwrite").saveAsTable("temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit")
spark.sql("""refresh table default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit""")

temp_d_time_bckt_XPO_WKLY_TB_ID=spark.sql("""
SELECT
distinct appl_nm,
min(tb_id) over (partition by tb_cd) as tb_id,
tb_cd from
d_time_bckt_tempdf
where src_nm='SLS' and appl_nm='MAT'
""")

temp_d_time_bckt_XPO_WKLY_TB_ID.registerTempTable("temp_d_time_bckt_XPO_WKLY_TB_ID")

temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_1=spark.sql("""
SELECT /*+ broadcast(b) */
az_cust_id,
az_geo_id,
brd_fam_id,
AZ_PROD_IND,
mkt_vol_ind,
az_mkt_id,
az_plan_id,
b.tb_id,
a.tb_cd,
src_sys_psbr_id,
az_team_id,
trgt_psbr_mkt_ind,
pdrp_opt_sta,
ama_sta,
pay_typ,
az_bot_acct_id,
bot_acct_nm,
chnl_nm,
CUR_NRX_CT,
PREV_NRX_CT,
CUR_nrx_ut,
PREV_NRX_ut,
CUR_nrx_factrd_ut,
PREV_nrx_factrd_ut,
CUR_TRX_CT,
PREV_TRX_CT,
CUR_Trx_ut,
PREV_TRX_ut,
CUR_Trx_factrd_ut,
PREV_Trx_factrd_ut,
CUR_nbrx_CT,
PREV_nbrx_CT,
CUR_NRX_EU,
PREV_NRX_EU,
CUR_TRX_EU,
PREV_TRX_EU,
cur_mkt_nrx_ct,
PREV_mkt_nrx_ct,
CUR_mkt_nrx_ut,
PREV_mkt_nrx_ut,
CUR_mkt_nrx_factrd_ut,
PREV_mkt_nrx_factrd_ut,
cur_mkt_Trx_ct,
PREV_mkt_Trx_ct,
CUR_mkt_Trx_ut,
PREV_mkt_Trx_ut,
CUR_mkt_Trx_factrd_ut,
PREV_mkt_Trx_factrd_ut,
cur_mkt_nBrx_ct,
PREV_mkt_nBrx_ct,
cur_mkt_nrx_EU,
PREV_mkt_nrx_EU,
cur_mkt_Trx_EU,
PREV_mkt_Trx_EU,
cur_bot_mkt_nrx_ct,
PREV_bot_mkt_nrx_ct,
CUR_bot_mkt_nrx_ut,
PREV_bot_mkt_nrx_ut,
CUR_bot_mkt_nrx_factrd_ut,
PREV_bot_mkt_nrx_factrd_ut,
cur_bot_mkt_Trx_ct,
PREV_bot_mkt_Trx_ct,
CUR_bot_mkt_Trx_ut,
PREV_bot_mkt_Trx_ut,
CUR_bot_mkt_Trx_factrd_ut,
PREV_bot_mkt_Trx_factrd_ut,
cur_bot_mkt_nBrx_ct,
PREV_bot_mkt_nBrx_ct,
cur_bot_mkt_nrx_EU,
PREV_bot_mkt_nrx_EU,
cur_bot_mkt_Trx_EU,
PREV_bot_mkt_Trx_EU,
brd_fam_ds,
brd_fam_type,
MKT_NM,
team_nm,
cust_clas,
cust_nm,
cust_typ,
cust_sta_cd,
pri_spec_cd,
mkt_spec_nm,
plan_nm,
frmy_tier,
frmy_sta,
frmy_posn,
load_dt
from default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit a
join temp_d_time_bckt_XPO_WKLY_TB_ID b
on a.tb_cd = b.tb_cd""")

temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_1.registerTempTable("temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_1")

#CREATION OF A TEMP TABLE WITH FILTERS FOR DECILE USED FOR FETCHING SEGMENTATION INFO AT CUST LEVEL
#spark.sql(""" drop table if exists temp_d_seg""")
#spark.sql(""" CREATE TABLE temp_d_seg STORED AS PARQUET  TBLPROPERTIES ('PARQUET.COMPRESSION'='SNAPPY') AS SELECT * FROM us_commercial_datalake_app_matrix_$$env.d_seg  WHERE UPPER(SEG_TYPE)='DECILE' """)

#CREATION OF A TEMP TABLE WITH FILTERS FOR MARKET SPECIALTY USED FOR FETCHING MARKET SPECILATY INFO AT CUST LEVEL

temp_d_mkt_spec=spark.sql("""SELECT * FROM d_seg_tempdf  WHERE UPPER(SEG_TYPE)='MKT_SPEC'""")
temp_d_mkt_spec.registerTempTable("temp_d_mkt_spec")
#R3'19 Pass for Customer Segmentation

temp_brd_seg_cus=spark.sql("""
select /*+ broadcast(b) */ A.hcp_az_cust_id,A.az_mkt_id, B.seg_val_ds, A.seg_val_id
from
r_hcp_seg_tempdf A
join
d_seg_tempdf B
on
A.seg_val_id = B.seg_val_id
where
trim(lower(B.seg_type))='cust_grp'
group by 1, 2, 3, 4
""")

temp_brd_seg_cus.createOrReplaceTempView("temp_brd_seg_cus")

'''
temp_vna_seg = spark.sql("""
select
distinct
hcp_az_cust_id,
az_mkt_id,
mkt_nm,
seg_val_id,
seg_val_ds
FROM
us_commercial_datalake_app_matrix_$$env.r_cust_vna_mkt_seg
where cust_class = 'HCP'
""")

temp_vna_seg.createOrReplaceTempView("temp_vna_seg")
'''

#ADDING THE PROFILE INFORMATION (SEGMENTATION,MARKET SPECILATY,GEO_DESCRIPTION) TO THE WEEKLY AGGREGATE AND INSERTING THE DATA IN THE GEO SALES INTERMEDIATE TABLE
temp_itmd_sls_xpopd_aligt_geo_1=spark.sql("""
SELECT /*+ BROADCAST(B,MKT_SPEC) */
A.tb_id,
A.tb_cd,
A.az_team_id as team_id,
A.team_nm as team_nm,
geo.terr_ds as geo_terr_ds,
A.az_geo_id as geo_terr_id,
A.cust_nm as hcp_cust_ds,
A.az_cust_id as hcp_az_cust_id,
A.pri_spec_cd as pri_spec_id,
CST.pri_spec_desc as spec_ds,
nvl(B.seg_val_id,'28003') as seg_val_id,
nvl(B.seg_val_ds,'(8-10, 99)') as seg_val_ds,
'29099' as seg_vna_id,
'UN-SEGMENTED' as seg_vna_ds,
nvl(MKT_SPEC.SEG_VAL_ID,'22001') as mkt_spty_id,
nvl(A.mkt_spec_nm,'All Other Specialties') as mkt_spty_ds,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.frmy_sta,
A.frmy_posn as frmy_ds,
A.frmy_tier as cpay_tier_id,
A.frmy_posn ,
A.az_plan_id as plan_id,
A.plan_nm as plan_ds,
A.az_mkt_id,
A.MKT_NM,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.mkt_vol_ind,
A.az_prod_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.trgt_psbr_mkt_ind as trgt_fam_psbr_flg,
A.chnl_nm,
A.CUR_nbrx_CT ,
A.PREV_nbrx_CT ,
A.CUR_NRX_CT ,
A.PREV_NRX_CT ,
A.CUR_nrx_factrd_ut,
A.PREV_nrx_factrd_ut,
A.CUR_nrx_ut ,
A.PREV_NRX_ut ,
A.CUR_NRX_EU ,
A.PREV_NRX_EU ,
A.CUR_TRX_CT ,
A.PREV_TRX_CT ,
A.CUR_Trx_factrd_ut,
A.PREV_Trx_factrd_ut,
A.CUR_Trx_ut ,
A.PREV_TRX_ut ,
A.CUR_TRX_EU ,
A.PREV_TRX_EU ,
A.cur_mkt_nBrx_ct ,
A.PREV_mkt_nBrx_ct,
A.cur_mkt_nrx_ct ,
A.PREV_mkt_nrx_ct,
A.CUR_mkt_nrx_factrd_ut ,
A.PREV_mkt_nrx_factrd_ut ,
A.CUR_mkt_nrx_ut ,
A.PREV_mkt_nrx_ut ,
A.cur_mkt_nrx_eu ,
A.PREV_mkt_nrx_eu,
A.cur_mkt_Trx_ct ,
A.PREV_mkt_Trx_ct,
A.CUR_mkt_Trx_factrd_ut ,
A.PREV_mkt_Trx_factrd_ut ,
A.CUR_mkt_Trx_ut ,
A.PREV_mkt_Trx_ut ,
A.cur_mkt_trx_eu ,
A.PREV_mkt_trx_eu,
A.cur_bot_mkt_nBrx_ct ,
A.PREV_bot_mkt_nBrx_ct,
A.cur_bot_mkt_nrx_ct ,
A.PREV_bot_mkt_nrx_ct,
A.CUR_bot_mkt_nrx_factrd_ut ,
A.PREV_bot_mkt_nrx_factrd_ut ,
A.CUR_bot_mkt_nrx_ut ,
A.PREV_bot_mkt_nrx_ut ,
A.cur_bot_mkt_nrx_eu ,
A.PREV_bot_mkt_nrx_eu,
A.cur_bot_mkt_Trx_ct ,
A.PREV_bot_mkt_Trx_ct,
A.CUR_bot_mkt_Trx_factrd_ut ,
A.PREV_bot_mkt_Trx_factrd_ut ,
A.CUR_bot_mkt_Trx_ut ,
A.PREV_bot_mkt_Trx_ut ,
A.cur_bot_mkt_trx_eu ,
A.PREV_bot_mkt_trx_eu,
A.load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from
temp_xpo_pln_dyn_wkly_brd_fam_aggr_mkt_1 A
LEFT JOIN temp_brd_seg_cus B
ON A.az_cust_id=B.hcp_az_cust_id
and A.az_mkt_id=B.az_mkt_id
join
d_cust_tempdf CST
on
A.AZ_CUST_ID=CST.AZ_CUST_ID
LEFT JOIN  temp_d_mkt_spec MKT_SPEC
ON A.mkt_spec_nm=MKT_SPEC.seg_val_ds
JOIN
d_aligt_tempdf geo
on A.AZ_GEO_ID=geo.terr_id
""")

temp_itmd_sls_xpopd_aligt_geo_1.registerTempTable("temp_itmd_sls_xpopd_aligt_geo_1")

'''
LEFT JOIN
temp_vna_seg vna
ON A.az_cust_id=vna.hcp_az_cust_id and A.az_mkt_id = vna.az_mkt_id
'''
'''
NVL(vna.seg_val_id,'29099') as seg_vna_id,
NVL(vna.seg_val_ds,'UN-SEGMENTED') as seg_vna_ds,
'''


bridge_df=spark.sql("""select distinct iqvia_az_cust_id,'Y' as bridge_ind from temp_itmd_cust_bio_bridge_tempdf """)
bridge_df.createOrReplaceTempView('bridge_df')

table_name=table_list.filter(table_list.tableName=="temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit""")
else:
    print("table not found")
    
temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit=spark.sql("""
SELECT /*+ BROADCAST(bhvr1,prty1,bridge) */
A.tb_id,
A.tb_cd,
A.team_id,
A.team_nm,
A.geo_terr_ds,
A.geo_terr_id,
A.hcp_cust_ds,
A.hcp_az_cust_id,
A.pri_spec_id,
A.spec_ds,
A.seg_val_id,
A.seg_val_ds,
A.seg_vna_id,
A.seg_vna_ds,
NVL(bhvr1.bhvr_seg_id,'30999') as bhvr_seg_id,
NVL(bhvr1.bhvr_seg_ds,'No Tier') as bhvr_seg_ds,
NVL(prty1.prty_seg_id,'32999') as prty_seg_id,
NVL(prty1.prty_seg_ds,'UN-SEGMENTED') as prty_seg_ds,
A.mkt_spty_id,
A.mkt_spty_ds,
'-1' as bridge_az_cust_id,
nvl(bridge.bridge_ind,'N') as bridge_ind,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.frmy_sta,
A.frmy_ds,
A.cpay_tier_id,
A.frmy_posn ,
A.plan_id,
A.plan_ds,
A.az_mkt_id,
A.MKT_NM,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.mkt_vol_ind,
A.az_prod_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.trgt_fam_psbr_flg,
A.chnl_nm,
A.CUR_nbrx_CT ,
A.PREV_nbrx_CT ,
A.CUR_NRX_CT ,
A.PREV_NRX_CT ,
A.CUR_nrx_factrd_ut,
A.PREV_nrx_factrd_ut,
A.CUR_nrx_ut ,
A.PREV_NRX_ut ,
A.CUR_NRX_EU ,
A.PREV_NRX_EU ,
A.CUR_TRX_CT ,
A.PREV_TRX_CT ,
A.CUR_Trx_factrd_ut,
A.PREV_Trx_factrd_ut,
A.CUR_Trx_ut ,
A.PREV_TRX_ut ,
A.CUR_TRX_EU ,
A.PREV_TRX_EU ,
A.cur_mkt_nBrx_ct ,
A.PREV_mkt_nBrx_ct,
A.cur_mkt_nrx_ct ,
A.PREV_mkt_nrx_ct,
A.CUR_mkt_nrx_factrd_ut ,
A.PREV_mkt_nrx_factrd_ut ,
A.CUR_mkt_nrx_ut ,
A.PREV_mkt_nrx_ut ,
A.cur_mkt_nrx_eu ,
A.PREV_mkt_nrx_eu,
A.cur_mkt_Trx_ct ,
A.PREV_mkt_Trx_ct,
A.CUR_mkt_Trx_factrd_ut ,
A.PREV_mkt_Trx_factrd_ut ,
A.CUR_mkt_Trx_ut ,
A.PREV_mkt_Trx_ut ,
A.cur_mkt_trx_eu ,
A.PREV_mkt_trx_eu,
A.cur_bot_mkt_nBrx_ct ,
A.PREV_bot_mkt_nBrx_ct,
A.cur_bot_mkt_nrx_ct ,
A.PREV_bot_mkt_nrx_ct,
A.CUR_bot_mkt_nrx_factrd_ut ,
A.PREV_bot_mkt_nrx_factrd_ut ,
A.CUR_bot_mkt_nrx_ut ,
A.PREV_bot_mkt_nrx_ut ,
A.cur_bot_mkt_nrx_eu ,
A.PREV_bot_mkt_nrx_eu,
A.cur_bot_mkt_Trx_ct ,
A.PREV_bot_mkt_Trx_ct,
A.CUR_bot_mkt_Trx_factrd_ut ,
A.PREV_bot_mkt_Trx_factrd_ut ,
A.CUR_bot_mkt_Trx_ut ,
A.PREV_bot_mkt_Trx_ut ,
A.cur_bot_mkt_trx_eu ,
A.PREV_bot_mkt_trx_eu,
A.load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from
temp_itmd_sls_xpopd_aligt_geo_1 A
LEFT JOIN
r_cust_bhvr_brd_fam_seg_tempdf bhvr1
on
A.az_mkt_id = bhvr1.az_mkt_id and A.hcp_az_cust_id=bhvr1.az_cust_id and A.brd_fam_id=bhvr1.brd_fam_id
LEFT JOIN
r_cust_prty_brd_fam_seg_tempdf prty1
on
A.az_mkt_id = prty1.az_mkt_id and A.hcp_az_cust_id=prty1.az_cust_id and A.brd_fam_id=prty1.brd_fam_id
left join
bridge_df bridge
on
A.hcp_az_cust_id=bridge.iqvia_az_cust_id
""")

temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit.write.mode("overwrite").saveAsTable("temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit")
spark.sql("""refresh table default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit""")
### Creation of selective market based time bucket filter
mapping=spark.sql("""select val,attr1 from 
cfg_gner_parm_val_tempdf cfg
where lower(cfg.parm_type)='tb_mkt'
and   lower(cfg.tbl_nm)='sls_imtd_mkt_tb_filter'
 """)
mapping.createOrReplaceTempView("mapping")

tb_list=spark.sql("""select tb_cd from default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit group by 1 """)
tb_list.createOrReplaceTempView("tb_list")

mkt_list=spark.sql("""select mkt_nm,'1' flg from default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit group by 1""")
mkt_list.createOrReplaceTempView("mkt_list")

tb_mkt_1=spark.sql("""select /*+ broadcast(mapping) */ 
coalesce( tb_cd,val)tb_cd,if(attr1 is null,'1',attr1) attr1 
from  tb_list
left join 
mapping 
on lower(trim(tb_list.tb_cd))=lower(trim(mapping.val))""")
tb_mkt_1.createOrReplaceTempView("tb_mkt_1")

spark.conf.set("spark.sql.crossJoin.enabled","true")

tb_mkt_2=spark.sql("""
select /*+ broadcast(mkt_list) */ 
tb_cd,mkt_nm  from 
tb_mkt_1 join mkt_list on attr1=flg
where attr1 in('1')
union
select tb_cd,attr1 mkt_nm from tb_mkt_1 where attr1 not in('1') """)
tb_mkt_2.createOrReplaceTempView("tb_mkt_2")

 
table_name=table_list.filter(table_list.tableName=="temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit").collect()
if len(table_name)>0:
    spark.sql("""truncate table default.temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit""")
    spark.sql("""drop table if exists default.temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit""")
else:
    print("table not found")
    
temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit=spark.sql("""
SELECT /*+ BROADCAST(b) */
A.tb_id,
A.tb_cd,
A.team_id,
A.team_nm,
A.geo_terr_ds,
A.geo_terr_id,
A.hcp_cust_ds,
A.hcp_az_cust_id,
A.pri_spec_id,
A.spec_ds,
A.seg_val_id,
A.seg_val_ds,
A.seg_vna_id,
A.seg_vna_ds,
A.bhvr_seg_id,
A.bhvr_seg_ds,
A.prty_seg_id,
A.prty_seg_ds,
A.mkt_spty_id,
A.mkt_spty_ds,
A.bridge_az_cust_id,
A.bridge_ind,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.frmy_sta,
A.frmy_ds,
A.cpay_tier_id,
A.frmy_posn ,
A.plan_id,
A.plan_ds,
A.az_mkt_id,
A.MKT_NM,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.mkt_vol_ind,
A.az_prod_ind,
A.pdrp_opt_sta,
A.ama_sta,
A.trgt_fam_psbr_flg,
A.chnl_nm,
A.CUR_nbrx_CT ,
A.PREV_nbrx_CT ,
A.CUR_NRX_CT ,
A.PREV_NRX_CT ,
A.CUR_nrx_factrd_ut,
A.PREV_nrx_factrd_ut,
A.CUR_nrx_ut ,
A.PREV_NRX_ut ,
A.CUR_NRX_EU ,
A.PREV_NRX_EU ,
A.CUR_TRX_CT ,
A.PREV_TRX_CT ,
A.CUR_Trx_factrd_ut,
A.PREV_Trx_factrd_ut,
A.CUR_Trx_ut ,
A.PREV_TRX_ut ,
A.CUR_TRX_EU ,
A.PREV_TRX_EU ,
A.cur_mkt_nBrx_ct ,
A.PREV_mkt_nBrx_ct,
A.cur_mkt_nrx_ct ,
A.PREV_mkt_nrx_ct,
A.CUR_mkt_nrx_factrd_ut ,
A.PREV_mkt_nrx_factrd_ut ,
A.CUR_mkt_nrx_ut ,
A.PREV_mkt_nrx_ut ,
A.cur_mkt_nrx_eu ,
A.PREV_mkt_nrx_eu,
A.cur_mkt_Trx_ct ,
A.PREV_mkt_Trx_ct,
A.CUR_mkt_Trx_factrd_ut ,
A.PREV_mkt_Trx_factrd_ut ,
A.CUR_mkt_Trx_ut ,
A.PREV_mkt_Trx_ut ,
A.cur_mkt_trx_eu ,
A.PREV_mkt_trx_eu,
A.cur_bot_mkt_nBrx_ct ,
A.PREV_bot_mkt_nBrx_ct,
A.cur_bot_mkt_nrx_ct ,
A.PREV_bot_mkt_nrx_ct,
A.CUR_bot_mkt_nrx_factrd_ut ,
A.PREV_bot_mkt_nrx_factrd_ut ,
A.CUR_bot_mkt_nrx_ut ,
A.PREV_bot_mkt_nrx_ut ,
A.cur_bot_mkt_nrx_eu ,
A.PREV_bot_mkt_nrx_eu,
A.cur_bot_mkt_Trx_ct ,
A.PREV_bot_mkt_Trx_ct,
A.CUR_bot_mkt_Trx_factrd_ut ,
A.PREV_bot_mkt_Trx_factrd_ut ,
A.CUR_bot_mkt_Trx_ut ,
A.PREV_bot_mkt_Trx_ut ,
A.cur_bot_mkt_trx_eu ,
A.PREV_bot_mkt_trx_eu,
A.load_dt,
'$$data_dt' as data_dt,
'$$cycle_id' as cycl_id
from
default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit A
join  tb_mkt_2 b on  a.tb_cd=b.tb_cd and a.mkt_nm=b.mkt_nm """)

temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit.write.mode("overwrite").saveAsTable("temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit")
spark.sql("""refresh table default.temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit""")

temp_sls_tb_cur=spark.sql("""select distinct tb_id from d_time_bckt_tempdf tb where tb_type = 'CURR' """)
temp_sls_tb_cur.registerTempTable("temp_sls_tb_cur")
#dataframe to pick distinct from summary alignment
summ_df=spark.sql("""select distinct summ_az_team_id,summ_team_nm,summ_terr_desc,az_summ_terr_id,az_geo_terr_id from map_geog_fnct_summ_terr_pd_tempdf """)
summ_df.createOrReplaceTempView("summ_df")

geo_data_df = spark.sql("""select * from us_commercial_datalake_app_matrix_$$env.temp_itmd_sls_xpopd_aligt_geo_$$mktsplit limit 5""")
geo_data_df.write.mode("overwrite").parquet("s3://az-us-commercial-datalake-$$env/applications/matrix/intermediate/temp_itmd_sls_xpopd_aligt_geo_$$mktsplit/")

read_geo_data_df = spark.read.load("s3://az-us-commercial-datalake-$$env/applications/matrix/intermediate/temp_itmd_sls_xpopd_aligt_geo_$$mktsplit/")
read_geo_data_df.registerTempTable("read_geo_data_df")

empty_geo_df = spark.sql(""" select * from read_geo_data_df where 1=2""")
empty_geo_df.write.mode("overwrite").parquet("s3://az-us-commercial-datalake-$$env/applications/matrix/intermediate/temp_itmd_sls_xpopd_aligt_geo_$$mktsplit/")

#Adding summary alignment
spark.sql("""
INSERT INTO table us_commercial_datalake_app_matrix_$$env.temp_itmd_sls_xpopd_aligt_geo_$$mktsplit
SELECT /*+ BROADCAST(summ,tb) */
A.tb_id,
A.tb_cd,
summ.summ_az_team_id as team_id,
summ.summ_team_nm as team_nm,
summ.summ_terr_desc as geo_terr_ds,
summ.az_summ_terr_id as geo_terr_id,
A.hcp_cust_ds,
A.hcp_az_cust_id,
A.pri_spec_id,
A.spec_ds,
A.seg_val_id,
A.seg_val_ds,
A.seg_vna_id,
A.seg_vna_ds,
A.bhvr_seg_id,
A.bhvr_seg_ds,
A.prty_seg_id,
A.prty_seg_ds,
A.mkt_spty_id,
A.mkt_spty_ds,
A.bridge_az_cust_id,
A.bridge_ind,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.frmy_sta ,
A.cpay_tier_id ,
A.frmy_posn ,
A.frmy_ds ,
A.plan_id,
A.plan_ds,
A.az_mkt_id,
A.MKT_NM,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.mkt_vol_ind,
A.az_prod_ind,
A.pdrp_opt_sta,
A.chnl_nm,
A.ama_sta,
max(A.trgt_fam_psbr_flg) as trgt_fam_psbr_flg ,
max(A.CUR_nbrx_CT),
max(A.PREV_nbrx_CT) ,
max(A.CUR_NRX_CT),
max(A.PREV_NRX_CT),
max(A.CUR_nrx_factrd_ut),
max(A.PREV_nrx_factrd_ut),
max(A.CUR_nrx_ut) ,
max(A.PREV_NRX_ut) ,
max(A.CUR_NRX_EU) ,
max(A.PREV_NRX_EU),
max(A.CUR_TRX_CT) ,
max(A.PREV_TRX_CT) ,
max(A.CUR_Trx_factrd_ut),
max(A.PREV_Trx_factrd_ut),
max(A.CUR_Trx_ut) ,
max(A.PREV_TRX_ut) ,
max(A.CUR_TRX_EU) ,
max(A.PREV_TRX_EU),
max(A.cur_mkt_nBrx_ct),
max(A.PREV_mkt_nBrx_ct),
max(A.cur_mkt_nrx_ct),
max(A.PREV_mkt_nrx_ct),
max(A.CUR_mkt_nrx_factrd_ut),
max(A.PREV_mkt_nrx_factrd_ut),
max(A.CUR_mkt_nrx_ut),
max(A.PREV_mkt_nrx_ut),
max(A.cur_mkt_nrx_eu),
max(A.PREV_mkt_nrx_eu),
max(A.cur_mkt_Trx_ct) ,
max(A.PREV_mkt_Trx_ct),
max(A.CUR_mkt_Trx_factrd_ut) ,
max(A.PREV_mkt_Trx_factrd_ut) ,
max(A.CUR_mkt_Trx_ut) ,
max(A.PREV_mkt_Trx_ut ),
max(A.cur_mkt_trx_eu),
max(A.PREV_mkt_trx_eu),
max(A.cur_bot_mkt_nBrx_ct),
max(A.PREV_bot_mkt_nBrx_ct),
max(A.cur_bot_mkt_nrx_ct),
max(A.PREV_bot_mkt_nrx_ct),
max(A.CUR_bot_mkt_nrx_factrd_ut),
max(A.PREV_bot_mkt_nrx_factrd_ut) ,
max(A.CUR_bot_mkt_nrx_ut) ,
max(A.PREV_bot_mkt_nrx_ut) ,
max(A.cur_bot_mkt_nrx_eu),
max(A.PREV_bot_mkt_nrx_eu),
max(A.cur_bot_mkt_Trx_ct) ,
max(A.PREV_bot_mkt_Trx_ct),
max(A.CUR_bot_mkt_Trx_factrd_ut) ,
max(A.PREV_bot_mkt_Trx_factrd_ut) ,
max(A.CUR_bot_mkt_Trx_ut) ,
max(A.PREV_bot_mkt_Trx_ut),
max(A.cur_bot_mkt_trx_eu) ,
max(A.PREV_bot_mkt_trx_eu),
max(A.load_dt)
from
default.temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit A
JOIN summ_df summ
on A.geo_terr_id=summ.az_geo_terr_id
join temp_sls_tb_cur tb on tb.tb_id = a.tb_id
group by
A.tb_id,
A.tb_cd,
summ.summ_az_team_id,
summ.summ_team_nm,
summ.summ_terr_desc,
summ.az_summ_terr_id,
A.hcp_cust_ds,
A.hcp_az_cust_id,
A.pri_spec_id,
A.spec_ds,
A.seg_val_id,
A.seg_val_ds,
A.seg_vna_id,
A.seg_vna_ds,
A.bhvr_seg_id,
A.bhvr_seg_ds,
A.prty_seg_id,
A.prty_seg_ds,
A.mkt_spty_id,
A.mkt_spty_ds,
A.bridge_az_cust_id,
A.bridge_ind,
A.pay_typ,
A.az_bot_acct_id,
A.bot_acct_nm,
A.frmy_sta ,
A.cpay_tier_id ,
A.frmy_posn ,
A.frmy_ds ,
A.plan_id,
A.plan_ds,
A.az_mkt_id,
A.MKT_NM,
A.brd_fam_id ,
A.brd_fam_ds,
A.brd_fam_type,
A.mkt_vol_ind,
A.az_prod_ind,
A.pdrp_opt_sta,
A.chnl_nm,
A.ama_sta
""")

spark.sql("""
INSERT INTO table us_commercial_datalake_app_matrix_$$env.temp_itmd_sls_xpopd_aligt_geo_$$mktsplit
SELECT /*+ BROADCAST(tb) */
a.tb_id ,
a.tb_cd ,
team_id ,
team_nm ,
geo_terr_ds ,
geo_terr_id ,
hcp_cust_ds ,
hcp_az_cust_id ,
pri_spec_id ,
spec_ds ,
seg_val_id ,
seg_val_ds ,
seg_vna_id,
seg_vna_ds,
bhvr_seg_id,
bhvr_seg_ds,
prty_seg_id,
prty_seg_ds,
mkt_spty_id ,
mkt_spty_ds ,
bridge_az_cust_id,
bridge_ind,
pay_typ ,
az_bot_acct_id ,
bot_acct_nm ,
frmy_sta ,
cpay_tier_id ,
frmy_posn ,
frmy_ds,
plan_id ,
plan_ds ,
az_mkt_id ,
MKT_NM ,
brd_fam_id ,
brd_fam_ds ,
brd_fam_type ,
mkt_vol_ind ,
az_prod_ind ,
pdrp_opt_sta,
chnl_nm,
ama_sta,
trgt_fam_psbr_flg ,
CUR_nbrx_CT ,
PREV_nbrx_CT ,
CUR_NRX_CT ,
PREV_NRX_CT ,
CUR_nrx_factrd_ut ,
PREV_nrx_factrd_ut ,
CUR_nrx_ut ,
PREV_NRX_ut ,
CUR_NRX_EU ,
PREV_NRX_EU ,
CUR_TRX_CT ,
PREV_TRX_CT ,
CUR_Trx_factrd_ut ,
PREV_Trx_factrd_ut ,
CUR_Trx_ut ,
PREV_TRX_ut ,
CUR_TRX_EU ,
PREV_TRX_EU ,
cur_mkt_nBrx_ct ,
PREV_mkt_nBrx_ct ,
cur_mkt_nrx_ct ,
PREV_mkt_nrx_ct ,
CUR_mkt_nrx_factrd_ut ,
PREV_mkt_nrx_factrd_ut,
CUR_mkt_nrx_ut ,
PREV_mkt_nrx_ut ,
cur_mkt_nrx_eu ,
PREV_mkt_nrx_eu ,
cur_mkt_Trx_ct ,
PREV_mkt_Trx_ct ,
CUR_mkt_Trx_factrd_ut ,
PREV_mkt_Trx_factrd_ut,
CUR_mkt_Trx_ut ,
PREV_mkt_Trx_ut ,
cur_mkt_trx_eu ,
PREV_mkt_trx_eu ,
A.cur_bot_mkt_nBrx_ct ,
A.PREV_bot_mkt_nBrx_ct,
A.cur_bot_mkt_nrx_ct ,
A.PREV_bot_mkt_nrx_ct,
A.CUR_bot_mkt_nrx_factrd_ut ,
A.PREV_bot_mkt_nrx_factrd_ut ,
A.CUR_bot_mkt_nrx_ut ,
A.PREV_bot_mkt_nrx_ut ,
A.cur_bot_mkt_nrx_eu ,
A.PREV_bot_mkt_nrx_eu,
A.cur_bot_mkt_Trx_ct ,
A.PREV_bot_mkt_Trx_ct,
A.CUR_bot_mkt_Trx_factrd_ut ,
A.PREV_bot_mkt_Trx_factrd_ut ,
A.CUR_bot_mkt_Trx_ut ,
A.PREV_bot_mkt_Trx_ut ,
A.cur_bot_mkt_trx_eu ,
A.PREV_bot_mkt_trx_eu,
load_dt
from
default.temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit A
join temp_sls_tb_cur tb on tb.tb_id = A.tb_id
""")

#spark.sql("""drop table if exists default.temp_XPO_WKLY_EPA_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_PLN_DYN_WKLY_EPA_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_XPO_WKLY_TB_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_PLN_DYN_WKLY_TB_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_XPO_PLN_DYN_WKLY_$$mktsplit""")
#spark.sql(""" drop table if exists default.tb_chnl_views_dif_r_$$mktsplit""")
#spark.sql(""" drop table if exists default.tb_chnl_views_dif_agr_r_$$mktsplit""")
#spark.sql("""Drop table if exists default.tb_chnl_views_total_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_1_blk_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_$$mktsplit """)
#spark.sql(""" drop table if exists default.temp_itmd_sls_xpopd_aligt_geo_1_seg_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_itmd_sls_xpopd_aligt_geo_2_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_XPO_WKLY_TB_AGGR_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_PLN_DYN_WKLY_TB_AGGR_$$mktsplit""")
#spark.sql("""drop table if exists default.tb_chnl_views_dif_rnr_all_$$mktsplit""")
#spark.sql(""" drop table if exists default.temp_XPO_PLN_DYN_WKLY_BRD_FAM_AGGR_MKT_$$mktsplit""")

spark.sql("""refresh table us_commercial_datalake_app_matrix_$$env.temp_itmd_sls_xpopd_aligt_geo_$$mktsplit""")
#spark.stop()
#CommonUtils().copy_hdfs_to_s3("us_commercial_datalake_app_matrix_$$env.temp_itmd_sls_xpopd_aligt_geo_$$mktsplit")
