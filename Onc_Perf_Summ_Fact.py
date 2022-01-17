spark.sql("""set sources.commitProtocolClass=com.databricks.io.CommitProtocol""")
spark.sql("""set spark.sql.broadcastTimeout = 30000""")
spark.sql(""" set hive.exec.dynamic.partition.mode=nonstrict""")
spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true") 
#spark.conf.set("spark.sql.shuffle.partitions", 720) 
spark.sql(""" set hive.exec.dynamic.partition.mode=nonstrict """)
import pyspark.sql.functions as sf
import pandas as pd

temp_spsd_sp_sales= spark.sql("""
select
tb_id,
tb_cd,
team_id,
team_nm,
geo_terr_id,
geo_terr_ds,
az_cust_id,
cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
pdrp_opt_out_ind,
cur_sp_unit,
prev_sp_unit,
cur_sd_unit,
prev_sd_unit,
cur_unit,
prev_unit,
cur_net_ref_ct,
prev_net_ref_ct,
cur_actv_shp_pat_ct,
prev_actv_shp_pat_ct,
cur_den_pat_ct,
prev_den_pat_ct,
cur_new_sp_pat_ct,
prev_new_sp_pat_ct,
cur_new_wrtr_sp_ct,
prev_new_wrtr_sp_ct,
cur_new_wrtr_sd_ct,
prev_new_wrtr_sd_ct,
cur_new_wrtr_ct,
prev_new_wrtr_ct,
new_wrtr_sp_dt,
new_wrtr_sd_dt,
cur_sp_pat_inf,
prev_sp_pat_inf,
cur_sd_pat_inf,
prev_sd_pat_inf,
cur_pat_inf,
prev_pat_inf,
cur_sp_mg,
prev_sp_mg,
cur_sd_mg,
prev_sd_mg,
cur_mg,
prev_mg
from itmd_sls_spsd_onc_geo_tempdf 
where channel='TOTAL' and pdrp_opt_out_ind='N'
""")
print(temp_spsd_sp_sales.count())
temp_spsd_sp_hca_sales= spark.sql("""
select
tb_id,
tb_cd,
team_id,
team_nm,
geo_terr_id,
geo_terr_ds,
par_az_cust_id as az_cust_id,
'HCA' as cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
pdrp_opt_out_ind,
cur_sp_unit,
prev_sp_unit,
cur_sd_unit,
prev_sd_unit,
cur_unit,
prev_unit,
cur_net_ref_ct,
prev_net_ref_ct,
cur_actv_shp_pat_ct,
prev_actv_shp_pat_ct,
cur_den_pat_ct,
prev_den_pat_ct,
cur_new_sp_pat_ct,
prev_new_sp_pat_ct,
cur_new_wrtr_sp_ct,
prev_new_wrtr_sp_ct,
cur_new_wrtr_sd_ct,
prev_new_wrtr_sd_ct,
cur_new_wrtr_ct,
prev_new_wrtr_ct,
new_wrtr_sp_dt,
new_wrtr_sd_dt,
cur_sp_pat_inf,
prev_sp_pat_inf,
cur_sd_pat_inf,
prev_sd_pat_inf,
cur_pat_inf,
prev_pat_inf,
cur_sp_mg,
prev_sp_mg,
cur_sd_mg,
prev_sd_mg,
cur_mg,
prev_mg
from itmd_sls_spsd_onc_geo_tempdf a
where channel='SP' and pdrp_opt_out_ind='N'
""")
print(temp_spsd_sp_hca_sales.count())
temp_spsd_total= temp_spsd_sp_hca_sales.unionAll(temp_spsd_sp_sales)
temp_spsd_total.registerTempTable("temp_spsd_total")
print(temp_spsd_total.count())
temp_spsd_aggr= spark.sql("""
select
tb_id,
tb_cd,
team_id,
team_nm,
geo_terr_id,
geo_terr_ds,
az_cust_id,
cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
sum(cur_sp_unit) as cur_sp_unit,
sum(prev_sp_unit) as prev_sp_unit,
sum(cur_sd_unit) as cur_sd_unit ,
sum(prev_sd_unit) as prev_sd_unit,
sum(cur_sp_unit + cur_sd_unit) as cur_unit, 
sum(prev_sp_unit + prev_sd_unit) as prev_unit ,
sum(cur_net_ref_ct) as cur_net_ref_ct,
sum(prev_net_ref_ct) as prev_net_ref_ct ,
sum(cur_actv_shp_pat_ct) as cur_actv_shp_pat_ct,
sum(prev_actv_shp_pat_ct) as prev_actv_shp_pat_ct ,
sum(cur_new_sp_pat_ct) as cur_new_sp_pat_ct ,
sum(prev_new_sp_pat_ct) as prev_new_sp_pat_ct,
sum(cur_new_wrtr_sp_ct) as cur_new_wrtr_sp_ct ,
sum(prev_new_wrtr_sp_ct) as prev_new_wrtr_sp_ct,
sum(cur_new_wrtr_sd_ct) as cur_new_wrtr_sd_ct,
sum(prev_new_wrtr_sd_ct) as prev_new_wrtr_sd_ct ,
sum(cur_sp_pat_inf) as cur_sp_pat_inf ,
sum(prev_sp_pat_inf) as prev_sp_pat_inf ,
sum(cur_sd_pat_inf) as cur_sd_pat_inf ,
sum(prev_sd_pat_inf) as prev_sd_pat_inf ,
sum(cur_sp_pat_inf + cur_sd_pat_inf) as cur_pat_inf ,
sum(prev_sp_pat_inf + prev_sd_pat_inf) as prev_pat_inf,
sum(cur_sp_mg) as cur_sp_mg,
sum(prev_sp_mg) as prev_sp_mg ,
sum(cur_sd_mg) as cur_sd_mg,
sum(prev_sd_mg) as prev_sd_mg,
sum(cur_mg) as cur_mg ,
sum(cur_new_wrtr_ct) as cur_new_wrtr_ct ,
sum(prev_new_wrtr_ct) as prev_new_wrtr_ct ,
max(new_wrtr_sp_dt) as new_wrtr_sp_dt,
max(new_wrtr_sd_dt) as new_wrtr_sd_dt,
sum(prev_mg) as prev_mg
from temp_spsd_total a
group by tb_id,tb_cd,team_id,team_nm,geo_terr_id,geo_terr_ds,az_cust_id,cust_clas,az_mkt_id,mkt_nm,brd_id,brd_ds
""")
print(temp_spsd_aggr.count())
temp_spsd_aggr.registerTempTable('temp_spsd_aggr')

itmd_sd_prchs_pdrp_dt=spark.sql("""
select a.* from itmd_sd_prchs_dt_tempdf a
inner join (select distinct az_cust_id from d_cust_hcp_hca_tempdf where pdrp_opt_out_ind='N' ) b 
on a.az_cust_id=b.az_cust_id""")
itmd_sd_prchs_pdrp_dt.registerTempTable('itmd_sd_prchs_dt_pdrp_tb')


itmd_sd_prchs_dt=spark.sql("""
select brd_ds,
hiery_id as geo_id,
hiery_ds as geo_ds,
hiery_lvl as hiery_lvl_ds,
CASE WHEN hiery_lvl='TERR' then '1' 
     WHEN hiery_lvl='DIST' then '2' 
     WHEN hiery_lvl='REG' then '3' 
     WHEN hiery_lvl='AREA' then '4' 
     WHEN hiery_lvl='NAT' then '5' 
end as hiery_lvl_id,
az_cust_id,
ltst_prchas_dt,
'HCA' as cust_clas
from itmd_sd_prchs_dt_pdrp_tb a
join (select distinct * from r_geo_funcl_tempdf where hiery_type='GEO') b 
on a.az_geo_id=b.terr_id
""")


itmd_sd_prchs_dt_pd=itmd_sd_prchs_dt.toPandas()
temp_spsd_hiery= spark.sql("""
select
tb_id,
tb_cd,
team_id,
team_nm,
hiery_id as geo_id,
hiery_ds as geo_ds,
hiery_lvl as hiery_lvl_ds,
CASE WHEN hiery_lvl='TERR' then '1' 
     WHEN hiery_lvl='DIST' then '2' 
     WHEN hiery_lvl='REG' then '3' 
     WHEN hiery_lvl='AREA' then '4' 
     WHEN hiery_lvl='NAT' then '5' 
end as hiery_lvl_id,
az_cust_id,
cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
sum(cur_sp_unit) as cur_sp_unit,
sum(prev_sp_unit) as prev_sp_unit,
sum(cur_sd_unit) as cur_sd_unit,
sum(prev_sd_unit) as prev_sd_unit,
sum(cur_unit) as cur_unit,
sum(prev_unit) as prev_unit ,
sum(cur_net_ref_ct) as cur_net_ref_ct,
sum(prev_net_ref_ct) as prev_net_ref_ct ,
sum(cur_actv_shp_pat_ct) as cur_actv_shp_pat_ct,
sum(prev_actv_shp_pat_ct) as prev_actv_shp_pat_ct,
sum(cur_new_sp_pat_ct) as cur_new_sp_pat_ct,
sum(prev_new_sp_pat_ct) as prev_new_sp_pat_ct ,
sum(cur_new_wrtr_sp_ct) as cur_new_wrtr_sp_ct ,
sum(prev_new_wrtr_sp_ct) as prev_new_wrtr_sp_ct,
sum(cur_new_wrtr_sd_ct) as cur_new_wrtr_sd_ct,
sum(prev_new_wrtr_sd_ct) as prev_new_wrtr_sd_ct ,
sum(cur_new_wrtr_ct) as cur_new_wrtr_ct ,
sum(prev_new_wrtr_ct) as prev_new_wrtr_ct ,
Sum(cur_sp_pat_inf) as cur_sp_pat_inf,
sum(prev_sp_pat_inf) as prev_sp_pat_inf,
sum(cur_sd_pat_inf) as cur_sd_pat_inf,
sum(prev_sd_pat_inf) as prev_sd_pat_inf ,
sum(cur_pat_inf) as cur_pat_inf ,
sum(prev_pat_inf) as prev_pat_inf ,
sum(cur_sp_mg) as cur_sp_mg ,
sum(prev_sp_mg) as prev_sp_mg,
sum(cur_sd_mg) as cur_sd_mg,
max(new_wrtr_sp_dt) as new_wrtr_sp_dt,
max(new_wrtr_sd_dt) as new_wrtr_sd_dt,
sum(prev_sd_mg) as prev_sd_mg ,
sum(cur_mg) as cur_mg ,
sum(prev_mg) as prev_mg from 
(select a.*,b.* from (select
tb_id,
tb_cd,
team_id,
team_nm,
geo_terr_id,
az_cust_id,
cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
sum(cur_sp_unit) as cur_sp_unit,
sum(prev_sp_unit) as prev_sp_unit,
sum(cur_sd_unit) as cur_sd_unit,
sum(prev_sd_unit) as prev_sd_unit,
sum(cur_unit) as cur_unit,
sum(prev_unit) as prev_unit ,
sum(cur_net_ref_ct) as cur_net_ref_ct,
sum(prev_net_ref_ct) as prev_net_ref_ct ,
sum(cur_actv_shp_pat_ct) as cur_actv_shp_pat_ct,
sum(prev_actv_shp_pat_ct) as prev_actv_shp_pat_ct,
sum(cur_new_sp_pat_ct) as cur_new_sp_pat_ct,
sum(prev_new_sp_pat_ct) as prev_new_sp_pat_ct ,
sum(cur_new_wrtr_sp_ct) as cur_new_wrtr_sp_ct ,
sum(prev_new_wrtr_sp_ct) as prev_new_wrtr_sp_ct,
sum(cur_new_wrtr_sd_ct) as cur_new_wrtr_sd_ct,
max(new_wrtr_sp_dt) as new_wrtr_sp_dt,
max(new_wrtr_sd_dt) as new_wrtr_sd_dt,
sum(prev_new_wrtr_sd_ct) as prev_new_wrtr_sd_ct ,
sum(cur_new_wrtr_ct) as cur_new_wrtr_ct ,
sum(prev_new_wrtr_ct) as prev_new_wrtr_ct ,
Sum(cur_sp_pat_inf) as cur_sp_pat_inf,
sum(prev_sp_pat_inf) as prev_sp_pat_inf,
sum(cur_sd_pat_inf) as cur_sd_pat_inf,
sum(prev_sd_pat_inf) as prev_sd_pat_inf ,
sum(cur_pat_inf) as cur_pat_inf ,
sum(prev_pat_inf) as prev_pat_inf ,
sum(cur_sp_mg) as cur_sp_mg ,
sum(prev_sp_mg) as prev_sp_mg,
sum(cur_sd_mg) as cur_sd_mg,
sum(prev_sd_mg) as prev_sd_mg ,
sum(cur_mg) as cur_mg ,
sum(prev_mg) as prev_mg
from  temp_spsd_aggr a 
group by tb_id,
tb_cd,
team_id,
team_nm,
geo_terr_id,
az_cust_id,
cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds ) a
join (select distinct * from r_geo_funcl_tempdf where hiery_type='GEO') b on a.geo_terr_id=b.terr_id ) a
group by tb_id,
tb_cd,
team_id,
team_nm,
geo_id,
geo_ds,
hiery_lvl_ds,
hiery_lvl_id,
az_cust_id,
cust_clas,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds
""")

temp_spsd_hiery.registerTempTable('temp_spsd_hiery_tb')



df=temp_spsd_hiery.select([c for c in temp_spsd_hiery.columns if c in ['az_cust_id','tb_id','tb_cd','geo_id','cust_clas','cur_unit','cur_sd_unit','az_mkt_id','brd_id','brd_ds','mkt_nm','prev_unit','cur_pat_inf','prev_pat_inf','cur_new_sp_pat_ct','prev_new_sp_pat_ct','new_wrtr_sp_dt','new_wrtr_sd_dt']])
raw_data=df.toPandas()
d_time_bckt_tempdf=spark.sql("""select * from d_time_bckt_tempdf """)
d_time_bckt_tempdf_pd=d_time_bckt_tempdf.toPandas()
d_time_bckt_tempdf_pd=d_time_bckt_tempdf_pd[((d_time_bckt_tempdf_pd['appl_nm']=='MAT')&(d_time_bckt_tempdf_pd['src_nm']=='SPSD')&(d_time_bckt_tempdf_pd['tb_type']=='CURR'))]


raw_data=raw_data.merge(d_time_bckt_tempdf_pd[['tb_id','strt_dt','end_dt']].drop_duplicates(),how='left',on='tb_id')

def sel_all_10(df,grp_col,agg_col1,agg_col2,flag):
  df1=df.drop_duplicates()
  df1=df1[~df1.isna()]
  df1['cur_rank']=df1.sort_values(agg_col1, ascending=False).groupby(grp_col).cumcount() + 1
  df1['prev_rank']=df1.sort_values(agg_col2, ascending=False).groupby(grp_col).cumcount() + 1
  df1['cur_rank']=df1['cur_rank'].astype(int)
  df1['prev_rank']=df1['prev_rank'].astype(int)
  df1['cust_flag'] = flag
  return df1.reset_index()
  

def sel_trending(df,grp_col,agg_col1,agg_col2,flag,lim):
  df1=df.drop_duplicates()
  df1=df1[~df1.isna()]
  df1[agg_col1]=df1[agg_col1].astype(float)
  df1[agg_col2]=df1[agg_col2].astype(float)
  df1['agg_col']=df1[agg_col1]-df1[agg_col2]
  if flag=='TRENDING UP':
    df1['rank']=df1.groupby(grp_col)['agg_col'].rank(method="first", ascending=False)
  else:
    df1['rank']=df1.groupby(grp_col)['agg_col'].rank(method="first", ascending=False)
  df1['cust_flag'] = flag
  return df1[df1['rank']<=lim].reset_index()
  
  
def sel_no_srt(df,grp_col,agg_col1,agg_col2,flag,lim):
  df1=df.drop_duplicates()
  df1=df1[~df1.isna()]
  df1=df1[df1[agg_col2]==0]
  #df1=df1[df1[agg_col1]!=0]
  df1['agg_col']=df1[agg_col1]
  df1['rank']=df1.groupby(grp_col)['agg_col'].rank(method="first", ascending=False)
  df1['cust_flag'] = flag
  return df1[df1['rank']<=lim].reset_index()

def sel_new_pres(df,grp_col,agg_col1,agg_col2,flag,lim,cust_class):
  df1=df.drop_duplicates()
  df1=df1[~df1.isna()]
  df1_hcp=df1[df1.cust_clas==cust_class]
  df1=df1[~df1[agg_col2].isna()]
  df1['new_dt']=((pd.DatetimeIndex(df1[agg_col2]).year)*10000
  +(pd.DatetimeIndex(df1[agg_col2]).month)*100
  +(pd.DatetimeIndex(df1[agg_col2]).day)*1)
  df1=df1[(df1['new_dt']>=df1['strt_dt'].astype('int'))&(df1['new_dt']<=df1['end_dt'].astype('int'))]
  df1['agg_col']=df1[agg_col1]
  df1['rank']=df1.groupby(grp_col)['agg_col'].rank(method="first", ascending=False)
  df1['cust_flag'] = flag
  return df1[df1['rank']<=lim].reset_index()


oral_br_list=spark.sql("""
select distinct val from cfg_gner_parm_val_tempdf where attr1='ORAL' and tbl_nm='brand_infusion' and parm_typ='brd_typ' """)
oral_br_list_pd=oral_br_list.toPandas()
oral_br_list=oral_br_list_pd.val.tolist()
inf_br_list=spark.sql("""
select distinct val from cfg_gner_parm_val_tempdf where attr1='INFUSION' and tbl_nm='brand_infusion' and parm_typ='brd_typ' """)
inf_br_list_pd=inf_br_list.toPandas()
inf_br_list=inf_br_list_pd.val.tolist()

oral_raw=raw_data[raw_data.brd_ds.isin(oral_br_list)]
inf_raw=raw_data[raw_data.brd_ds.isin(inf_br_list)]
grp_col=['tb_id','tb_cd','geo_id','cust_clas','az_mkt_id','brd_id']


agg_col1='cur_unit'
agg_col2='prev_unit'
all_oral=sel_all_10(oral_raw,grp_col,agg_col1,agg_col2,'ALL')
print(all_oral.shape[0])
agg_col1='cur_pat_inf'
agg_col2='prev_pat_inf'
all_inf=sel_all_10(inf_raw,grp_col,agg_col1,agg_col2,'ALL')
print(all_inf.shape[0])
all_results=all_oral.append(all_inf)
print(all_results.shape[0])
top_results=all_results[all_results['cur_rank']<=10]
top_results['cust_flag']='TOP'
print(top_results.shape[0])
all_top_df=all_results.append(top_results)
all_top_df = all_top_df[['tb_id','tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds', 'cur_sd_unit', 'cur_unit', 'prev_unit',
       'cur_new_sp_pat_ct', 'prev_new_sp_pat_ct', 'cur_pat_inf',
       'prev_pat_inf','new_wrtr_sp_dt','new_wrtr_sd_dt', 'cur_rank', 'prev_rank', 'cust_flag']]


agg_col1='cur_unit'
agg_col2='prev_unit'
trend_up_oral=sel_trending(oral_raw,grp_col,agg_col1,agg_col2,'TRENDING UP',10)
print(trend_up_oral.shape[0])

agg_col1='cur_pat_inf'
agg_col2='prev_pat_inf'
trend_up_inf=sel_trending(inf_raw,grp_col,agg_col1,agg_col2,'TRENDING UP',10)
print(trend_up_inf.shape[0])

agg_col2='cur_unit'
agg_col1='prev_unit'
trend_dn_oral=sel_trending(oral_raw,grp_col,agg_col1,agg_col2,'TRENDING DOWN',10)
print(trend_dn_oral.shape[0])

agg_col2='cur_pat_inf'
agg_col1='prev_pat_inf'
trend_dn_inf=sel_trending(inf_raw,grp_col,agg_col1,agg_col2,'TRENDING DOWN',10)
print(trend_dn_inf.shape[0])


agg_col2='cur_new_sp_pat_ct'
agg_col1='prev_new_sp_pat_ct'
no_starts=sel_no_srt(raw_data,grp_col,agg_col1,agg_col2,'No New Patient starts',10)
print(no_starts.shape[0])

agg_col2='new_wrtr_sp_dt'
agg_col1='cur_unit'
oral_new_srt_hcp=sel_new_pres(oral_raw,grp_col,agg_col1,agg_col2,'Prescribers with first script',10,'HCP')
agg_col2='new_wrtr_sd_dt'
agg_col1='cur_unit'
oral_new_srt_hca=sel_new_pres(oral_raw,grp_col,agg_col1,agg_col2,'Prescribers with first script',10,'HCA')
agg_col2='new_wrtr_sp_dt'
agg_col1='cur_pat_inf'
inf_new_srt_hcp=sel_new_pres(inf_raw,grp_col,agg_col1,agg_col2,'Prescribers with first script',10,'HCP')
agg_col2='new_wrtr_sd_dt'
agg_col1='cur_pat_inf'
inf_new_srt_hca=sel_new_pres(inf_raw,grp_col,agg_col1,agg_col2,'Prescribers with first script',10,'HCA')
new_srt = inf_new_srt_hca.append(inf_new_srt_hcp.append(oral_new_srt_hcp.append(oral_new_srt_hca)))
print(new_srt.shape[0])


other_df=trend_up_inf.append(trend_dn_inf.append(trend_up_oral.append(trend_dn_oral.append(no_starts.append(new_srt)))))
latest_prchs_acc=itmd_sd_prchs_dt_pd[itmd_sd_prchs_dt_pd.brd_ds.isin(inf_br_list)].sort_values('ltst_prchas_dt',ascending = False).groupby(['brd_ds','geo_id','geo_ds']).head(10)
latest_prchs_acc.reset_index()
latest_prchs_acc['cust_flag']='Accounts with recent purchase'
latest_prchs_acc_df=spark.createDataFrame(latest_prchs_acc)
latest_prchs_acc_df.createOrReplaceTempView('latest_prchs_acc_tb')

intd_prchs1=spark.sql("""
select a.brd_ds, a.geo_id, a.geo_ds, a.hiery_lvl_ds, a.hiery_lvl_id,
       a.az_cust_id, a.ltst_prchas_dt, a.cust_clas, a.cust_flag,b.tb_id,b.tb_cd,b.az_mkt_id,
       b.mkt_nm, b.brd_id from latest_prchs_acc_tb a
       left join
(select distinct tb_id,tb_cd, geo_id, cust_clas, az_mkt_id,
       mkt_nm, brd_id, brd_ds from temp_spsd_hiery_tb) b
       on a.brd_ds=b.brd_ds
       and a.geo_id=b.geo_id
       and a.cust_clas=b.cust_clas""")
intd_prchs1.createOrReplaceTempView('intd_prchs1_tb')
intd_prchs2=spark.sql("""
select a.brd_ds, a.geo_id, a.geo_ds,a.az_cust_id, a.ltst_prchas_dt, a.cust_clas, a.cust_flag,a.tb_id,a.tb_cd,a.az_mkt_id,
       a.mkt_nm, a.brd_id,b.cur_sd_unit, b.cur_unit, b.prev_unit,
       b.cur_new_sp_pat_ct, b.prev_new_sp_pat_ct, b.cur_pat_inf,
       b.prev_pat_inf, b.new_wrtr_sp_dt,b.new_wrtr_sd_dt from intd_prchs1_tb a
       left join
(select distinct tb_id,tb_cd, geo_id, az_cust_id, cust_clas, az_mkt_id,
       mkt_nm, brd_id, brd_ds, cur_sd_unit, cur_unit, prev_unit,
       cur_new_sp_pat_ct, prev_new_sp_pat_ct, cur_pat_inf,
       prev_pat_inf, new_wrtr_sp_dt,new_wrtr_sd_dt from temp_spsd_hiery_tb) b
       on a.brd_ds=b.brd_ds
       and a.geo_id=b.geo_id
       and a.cust_clas=b.cust_clas
       and a.tb_id=b.tb_id
       and a.tb_cd=b.tb_cd
       and a.az_cust_id=b.az_cust_id
       and a.az_mkt_id=b.az_mkt_id""")
intd_prchs2.createOrReplaceTempView('intd_prchs2_tb')
intd_prchs_pd=intd_prchs2.toPandas()
intd_prchs_pd[['cur_sd_unit','cur_unit','prev_unit','cur_new_sp_pat_ct','prev_new_sp_pat_ct','cur_pat_inf','prev_pat_inf']]=intd_prchs_pd[['cur_sd_unit','cur_unit','prev_unit','cur_new_sp_pat_ct','prev_new_sp_pat_ct','cur_pat_inf','prev_pat_inf']].fillna(0)



import numpy as np
other_df['ltst_prchas_dt']=np.nan
other_df['ltst_prchas_dt']= pd.to_datetime(other_df['ltst_prchas_dt'])
other_df=other_df[['tb_id','tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds', 'cur_sd_unit', 'cur_unit', 'prev_unit',
       'cur_new_sp_pat_ct', 'prev_new_sp_pat_ct', 'cur_pat_inf',
       'prev_pat_inf', 'cust_flag','new_wrtr_sp_dt','new_wrtr_sd_dt','ltst_prchas_dt']].append(intd_prchs_pd[['tb_id','tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds', 'cur_sd_unit', 'cur_unit', 'prev_unit',
       'cur_new_sp_pat_ct', 'prev_new_sp_pat_ct', 'cur_pat_inf',
       'prev_pat_inf', 'cust_flag','new_wrtr_sp_dt','new_wrtr_sd_dt','ltst_prchas_dt']])
	   
	   
print(other_df.shape[0])
print(intd_prchs_pd.shape[0])
other_df_rn=other_df.merge(all_results[['tb_id','tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds', 'cur_rank', 'prev_rank']].drop_duplicates(),how='left',on=['tb_id','tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds'])
print(other_df_rn.shape[0])
print(other_df_rn[other_df_rn.cust_flag=='Accounts with recent purchase'].shape[0])
all_top_df['ltst_prchas_dt']=np.nan	   
all_top_df['ltst_prchas_dt']=pd.to_datetime(all_top_df['ltst_prchas_dt'])
final_df=all_top_df[['tb_id', 'tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds', 'cur_sd_unit', 'cur_unit', 'prev_unit',
       'cur_new_sp_pat_ct', 'prev_new_sp_pat_ct', 'cur_pat_inf',
       'prev_pat_inf', 'cust_flag','cur_rank', 'prev_rank','ltst_prchas_dt']].append(other_df_rn[['tb_id', 'tb_cd', 'geo_id', 'az_cust_id', 'cust_clas', 'az_mkt_id',
       'mkt_nm', 'brd_id', 'brd_ds', 'cur_sd_unit', 'cur_unit', 'prev_unit',
       'cur_new_sp_pat_ct', 'prev_new_sp_pat_ct', 'cur_pat_inf',
       'prev_pat_inf', 'cust_flag', 'cur_rank', 'prev_rank','ltst_prchas_dt']])

final_df=final_df.astype(str)
print(final_df.shape[0])
final_df=final_df.drop_duplicates()
final_df=spark.createDataFrame(final_df)
final_df.registerTempTable('final_df')


df=spark.sql("""
select * from 
us_commercial_datalake_app_matrix_obu_$$env.f_mat_onc_sls_spsd_geo
where cycl_id='$$cycle_id' """)
df.registerTempTable('nat_sum')

output=spark.sql("""
select a.tb_id, a.tb_cd,a.geo_id, a.az_cust_id, a.cust_clas, a.az_mkt_id,
       a.mkt_nm, a.brd_id, a.brd_ds, a.cur_sd_unit, a.cur_unit,a.prev_unit,
       a.cur_new_sp_pat_ct,a.prev_new_sp_pat_ct, a.cur_pat_inf as cur_inf,
       a.prev_pat_inf as prev_inf, a.cust_flag as cust_flg,a.cur_rank as cur_prty, a.prev_rank as prev_prty,b.geo_ds,b.hiery_lvl_id,b.hiery_lvl_ds,b.cur_sp_unit,b.prev_sp_unit,b.prev_sd_unit,b.cur_net_ref_ct,b.prev_net_ref_ct,b.cur_new_sp_pat_ct as cur_new_pat_strt_sp_ct,b.prev_new_sp_pat_ct as prev_new_pat_strt_sp_ct,b.cur_sp_pat_inf as cur_sp_inf,
 b.prev_sp_pat_inf as prev_sp_inf,
b.cur_sd_pat_inf as cur_sd_inf,
 b.prev_sd_pat_inf as prev_sd_inf,
 b.cur_sp_mg,	
b.prev_sp_mg,
b.cur_sd_mg	,
b.prev_sd_mg,
b.cur_mg,
b.prev_mg,
c.cur_geo_unit,
c.cur_geo_sp_unit,
c.cur_geo_sd_unit,
c.cur_geo_inf,
c.cur_geo_sp_inf,
c.cur_geo_sd_inf,
a.ltst_prchas_dt as metric_value_1,
c.src_load_dt
from final_df a
left join temp_spsd_hiery_tb b
on a.tb_id=b.tb_id and a.tb_cd=b.tb_cd and a.geo_id=b.geo_id and a.az_cust_id=b.az_cust_id and a.cust_clas=b.cust_clas and a.az_mkt_id=b.az_mkt_id and a.mkt_nm=b.mkt_nm and a.brd_id=b.brd_id and a.brd_ds=b.brd_ds
join (select tb_id,
tb_cd,
geo_id,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
src_load_dt,
sum(cur_unit) as cur_geo_unit,
sum(cur_sp_unit) as cur_geo_sp_unit,
sum(cur_sd_unit) as cur_geo_sd_unit,
sum(cur_inf) as cur_geo_inf,
sum(cur_sp_inf) as cur_geo_sp_inf,
sum(cur_sd_inf) as cur_geo_sd_inf 
from nat_sum c
group by 
tb_id,
tb_cd,
geo_id,
az_mkt_id,
mkt_nm,
brd_id,
brd_ds,
src_load_dt) c
on a.tb_id=c.tb_id and 
a.tb_cd=c.tb_cd and
a.geo_id=c.geo_id and
a.az_mkt_id=c.az_mkt_id and
a.mkt_nm=c.mkt_nm and
a.brd_id=c.brd_id and
a.brd_ds=c.brd_ds
""").coalesce(500)
output.registerTempTable('output_tb')


output_f=spark.sql("""select tb_id, tb_cd,
geo_id, az_cust_id, cust_clas, az_mkt_id,
mkt_nm, brd_id, brd_ds, 
COALESCE(cur_sd_unit,0) as cur_sd_unit, 
COALESCE(cur_unit,0) as cur_unit,
COALESCE(prev_unit,0) as prev_unit,
COALESCE(cur_new_sp_pat_ct,0) AS cur_new_sp_pat_ct,
COALESCE(prev_new_sp_pat_ct,0) as prev_new_sp_pat_ct, 
COALESCE(cur_inf,0) as cur_inf,
COALESCE(prev_inf,0) as prev_inf,
cust_flg,cur_prty,prev_prty,
case when a.geo_ds is null then b.hiery_ds end as geo_ds,
case when a.hiery_lvl_ds is null then b.hiery_lvl end as hiery_lvl_ds,
CASE WHEN a.hiery_lvl_id is null and hiery_lvl='TERR' then '1' 
     WHEN a.hiery_lvl_id is null and hiery_lvl='DIST' then '2' 
     WHEN a.hiery_lvl_id is null and hiery_lvl='REG' then '3' 
     WHEN a.hiery_lvl_id is null and hiery_lvl='AREA' then '4' 
     WHEN a.hiery_lvl_id is null and hiery_lvl='NAT' then '5'
else a.hiery_lvl_id end as hiery_lvl_id,
COALESCE(cur_sp_unit,0) AS cur_sp_unit,
COALESCE(prev_sp_unit,0) AS prev_sp_unit,
COALESCE(prev_sd_unit,0) AS prev_sd_unit,
COALESCE(cur_net_ref_ct,0) AS cur_net_ref_ct,
COALESCE(prev_net_ref_ct,0) AS prev_net_ref_ct,
COALESCE(cur_new_pat_strt_sp_ct,0) AS cur_new_pat_strt_sp_ct,
COALESCE(prev_new_pat_strt_sp_ct,0) AS prev_new_pat_strt_sp_ct,
COALESCE(cur_sp_inf,0) AS cur_sp_inf,
COALESCE(prev_sp_inf,0) AS prev_sp_inf,
COALESCE(cur_sd_inf,0) AS cur_sd_inf,
COALESCE(prev_sd_inf,0) AS prev_sd_inf,
COALESCE(cur_sp_mg,0) AS cur_sp_mg,	
COALESCE(prev_sp_mg,0) AS prev_sp_mg,
COALESCE(cur_sd_mg,0) AS cur_sd_mg,
COALESCE(prev_sd_mg,0) AS prev_sd_mg,
COALESCE(cur_mg,0) AS cur_mg,
COALESCE(prev_mg,0) AS prev_mg,
COALESCE(cur_geo_unit,0) AS cur_geo_unit,
COALESCE(cur_geo_sp_unit,0) AS cur_geo_sp_unit,
COALESCE(cur_geo_sd_unit,0) AS cur_geo_sd_unit,
COALESCE(cur_geo_inf,0) AS cur_geo_inf,
COALESCE(cur_geo_sp_inf,0) AS cur_geo_sp_inf,
COALESCE(cur_geo_sd_inf,0) AS cur_geo_sd_inf,
metric_value_1,
src_load_dt 
from output_tb a
left join
(select distinct hiery_id,hiery_ds,hiery_lvl from r_geo_funcl_tempdf where hiery_type='GEO' and hiery_id is not null) b
on a.geo_id=b.hiery_id""")
output_f.registerTempTable('output_tb2')


spark.sql("""
INSERT OVERWRITE TABLE us_commercial_datalake_app_matrix_obu_$$env.f_mat_onc_cust_attr partition(data_dt,cycl_id)
select 
tb_id							,
tb_cd							,
geo_id							,
geo_ds							,
hiery_lvl_id					,
hiery_lvl_ds					,
az_mkt_id						,
mkt_nm							,
brd_id as brd_id							,
brd_ds							,
cust_clas						,
cust_flg 						,
az_cust_id						,
cur_prty						,
prev_prty 						,
cur_unit						,
prev_unit						,
cur_sp_unit						,
prev_sp_unit					,
cur_sd_unit						,
prev_sd_unit					,
cur_geo_unit					,
cur_geo_sp_unit 				,
cur_geo_sd_unit 				,
cur_net_ref_ct					,
prev_net_ref_ct					,
cur_new_pat_strt_sp_ct			,
prev_new_pat_strt_sp_ct			,
cur_inf							,
prev_inf						,
cur_sp_inf						,
prev_sp_inf						,
cur_sd_inf						,
prev_sd_inf						,
cur_geo_inf						,
cur_geo_sp_inf					,
cur_geo_sd_inf 					,
cur_sp_mg						,
prev_sp_mg						,
cur_sd_mg						,
prev_sd_mg						,
cur_mg							,
prev_mg,
metric_value_1,
'' as metric_value_2,
'' as metric_value_3,
'' as metric_value_4,
'' as metric_value_5,
'' as metric_value_6,
'' as metric_value_7,
'' as metric_value_8,
'' as metric_value_9,
'' as metric_value_10,
src_load_dt,
"$$data_dt" as data_dt,
"$$cycle_id" as cycl_id
from output_tb2""")

spark.sql("refresh table us_commercial_datalake_app_matrix_obu_$$env.f_mat_onc_cust_attr")
