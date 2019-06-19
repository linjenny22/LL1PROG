import pandas as pd
import pyodbc

conn = pyodbc.connect(DSN='APT Analytics') #32 bit python 2

sql1 = 'Select epin, req_cont_for_goods_services,req_contract_explanation,srvc_const_occ_multi_site,srvc_occur_multi_sites_expl,sin_indiv_pro_service_proj,single_indiv_proj_desc,cont_ref_uniq_unusual_goods,unique_unusual_goods_expl FROM apt_rpt_tbl_frm_psr_n_nego_acq'

df = pd.read_sql_query(sql1, conn)

df.to_pickle(r'S:\Contracts\Research and IT\08 - MWBE\DAS Only\09 - Python and R Scripts\Development\FY18 MWBE Prog Files\Scripts\Optimized\LL1 Reporting - PRODUCTION\apt_rpt_tbl_frm_psr_n_nego_acq.pkl')

print df.columns