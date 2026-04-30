"""Parsea la tabla pegada por el usuario y encuentra triggers duplicados."""
import sys, re
from collections import defaultdict

class Tee:
    def __init__(self,*s):self.s=s
    def write(self,t):
        for x in self.s:
            try: x.write(t); x.flush()
            except: pass
    def flush(self):
        for x in self.s:
            try: x.flush()
            except: pass
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_dup_triggers_out.txt","w",encoding="utf-8"))

# Tabla pegada por el usuario (extraida)
DATA = """301|trg_outbox_cgtbarch_ats                 |dbCG.dbo.cgtbarch_ats                 ||true  |      29|
302|trg_outbox_cgtbauxi                     |dbCG.dbo.cgtbauxi                     ||true  |      33|
303|trg_outbox_cgtbaval_hipo_cdio           |dbCG.dbo.cgtbaval_hipo_cdio           ||true  |      28|
304|trg_outbox_cgtbcasi                     |dbCG.dbo.cgtbcasi                     ||true  |      29|
305|trg_outbox_cgtbcaut_cpla                |dbCG.dbo.cgtbcaut_cpla                ||true  |      29|
306|trg_outbox_cgtbcaut_dpla                |dbCG.dbo.cgtbcaut_dpla                ||true  |      29|
307|trg_outbox_cgtbcaut_even                |dbCG.dbo.cgtbcaut_even                ||true  |      29|
308|trg_outbox_cgtbccos                     |dbCG.dbo.cgtbccos                     ||true  |      33|
309|trg_outbox_cgtbcfac                     |dbCG.dbo.cgtbcfac                     ||true  |      29|
310|trg_outbox_cgtbcier_proc                |dbCG.dbo.cgtbcier_proc                ||true  |      29|
311|trg_outbox_cgtbcncl                     |dbCG.dbo.cgtbcncl                     ||true  |      29|
312|trg_outbox_cgtbcncl_deta                |dbCG.dbo.cgtbcncl_deta                ||true  |      29|
313|trg_outbox_cgtbconc                     |dbCG.dbo.cgtbconc                     ||true  |      29|
314|trg_outbox_cgtbdasi                     |dbCG.dbo.cgtbdasi                     ||true  |      29|
315|trg_outbox_cgtbdcto_fact                |dbCG.dbo.cgtbdcto_fact                ||true  |      29|
316|trg_outbox_cgtbdist_cost                |dbCG.dbo.cgtbdist_cost                ||true  |      29|
317|trg_outbox_cgtbelem_fina                |dbCG.dbo.cgtbelem_fina                ||true  |      29|
318|trg_outbox_cgtbexcl_cnta_b17            |dbCG.dbo.cgtbexcl_cnta_B17            ||true  |      29|
319|trg_outbox_cgtbfcos                     |dbCG.dbo.cgtbfcos                     ||true  |      29|
146|trg_outbox_cgtbgara_hipo_cdio           |dbCG.dbo.cgtbgara_hipo_cdio           ||true  |      31|
320|trg_outbox_cgtbgara_vehi_cdio           |dbCG.dbo.cgtbgara_vehi_cdio           ||true  |      29|
321|trg_outbox_cgtbiaas                     |dbCG.dbo.cgtbiaas                     ||true  |      29|
322|trg_outbox_cgtbpago_anti                |dbCG.dbo.cgtbpago_anti                ||true  |      29|
323|trg_outbox_cgtbpers                     |dbCG.dbo.cgtbpers                     ||true  |      29|
147|trg_outbox_cgtbprod_cnta_auto           |dbCG.dbo.cgtbprod_cnta_auto           ||true  |      31|
267|trg_outbox_cgtbprvd                     |dbCG.dbo.cgtbprvd                     ||true  |      20|
324|trg_outbox_cgtbrcaj_chic                |dbCG.dbo.cgtbrcaj_chic                ||true  |      29|
325|trg_outbox_cgtbrcaj_chic_deta           |dbCG.dbo.cgtbrcaj_chic_deta           ||true  |      29|
148|trg_outbox_cgtbrepo_anls_cnta           |dbCG.dbo.cgtbrepo_anls_cnta           ||true  |      31|
326|trg_outbox_cgtbrepo_bala_gene           |dbCG.dbo.cgtbrepo_bala_gene           ||true  |      29|
327|trg_outbox_cgtbrete                     |dbCG.dbo.cgtbrete                     ||true  |      29|
328|trg_outbox_cgtbsald_egre                |dbCG.dbo.cgtbsald_egre                ||true  |      29|
329|trg_outbox_cgtbsald_temp                |dbCG.dbo.cgtbsald_temp                ||true  |      29|
330|trg_outbox_cgtbscpb_sri                 |dbCG.dbo.cgtbscpb_sri                 ||true  |      29|
331|trg_outbox_cgtbtipo_docu_cdio           |dbCG.dbo.cgtbtipo_docu_cdio           ||true  |      29|
332|trg_outbox_cgtbtprd                     |dbCG.dbo.cgtbtprd                     ||true  |      29|
333|trg_outbox_cgtbucos                     |dbCG.dbo.cgtbucos                     ||true  |      29|
149|trg_outbox_crtbabno_extr                |dbCR.dbo.crtbabno_extr                ||true  |      31|
150|trg_outbox_crtbcart_calf_prov           |dbCR.dbo.crtbcart_calf_prov           ||true  |      31|
151|trg_outbox_crtbcaut_cred                |dbCR.dbo.crtbcaut_cred                ||true  |      31|
334|trg_outbox_crtbcaut_gara                |dbCR.dbo.crtbcaut_gara                ||true  |      28|
152|trg_outbox_crtbccbr_cred_judi           |dbCR.dbo.crtbccbr_cred_judi           ||true  |      31|
153|trg_outbox_crtbcdeb_cnta                |dbCR.dbo.crtbcdeb_cnta                ||true  |      31|
335|trg_outbox_crtbcgar_code_sibs           |dbCR.dbo.crtbcgar_code_sibs           ||true  |      28|
154|trg_outbox_crtbcobr_judi_deta           |dbCR.dbo.crtbcobr_judi_deta           ||true  |      31|
155|trg_outbox_crtbcobr_judi_dist           |dbCR.dbo.crtbcobr_judi_dist           ||true  |      31|
156|trg_outbox_crtbconv_pago                |dbCR.dbo.crtbconv_pago                ||true  |      31|
157|trg_outbox_crtbcred_autr_deta           |dbCR.dbo.crtbcred_autr_deta           ||true  |      31|
158|trg_outbox_crtbcred_liqd_diar           |dbCR.dbo.crtbcred_liqd_diar           ||true  |      31|
159|trg_outbox_crtbcred_part                |dbCR.dbo.crtbcred_part                ||true  |      31|
160|trg_outbox_crtbcred_plzo_venc           |dbCR.dbo.crtbcred_plzo_venc           ||true  |      31|
161|trg_outbox_crtbcred_prea_whts           |dbCR.dbo.crtbcred_prea_whts           ||true  |      31|
162|trg_outbox_crtbctrl_oper_ante_sibs      |dbCR.dbo.crtbctrl_oper_ante_sibs      ||true  |      31|
163|trg_outbox_crtbdbso_devo                |dbCR.dbo.crtbdbso_devo                ||true  |      31|
164|trg_outbox_crtbdeud_conv                |dbCR.dbo.crtbdeud_conv                ||true  |      31|
165|trg_outbox_crtbdevo_masi_deta           |dbCR.dbo.crtbdevo_masi_deta           ||true  |      31|
166|trg_outbox_crtbdocu_cred                |dbCR.dbo.crtbdocu_cred                ||true  |      31|
167|trg_outbox_crtbdsal_oper                |dbCR.dbo.crtbdsal_oper                ||true  |      31|
168|trg_outbox_crtbdvgo_cart_deta           |dbCR.dbo.crtbdvgo_cart_deta           ||true  |      31|
169|trg_outbox_crtbdvgo_cart_deta_diar      |dbCR.dbo.crtbdvgo_cart_deta_diar      ||true  |      31|
170|trg_outbox_crtbesta_conv_cred           |dbCR.dbo.crtbesta_conv_cred           ||true  |      31|
336|trg_outbox_crtbgara_pgre                |dbCR.dbo.crtbgara_pgre                ||true  |      28|
337|trg_outbox_crtbgara_real                |dbCR.dbo.crtbgara_real                ||true  |      28|
171|trg_outbox_crtbgest_cart_asig           |dbCR.dbo.crtbgest_cart_asig           ||true  |      31|
172|trg_outbox_crtbgest_cred                |dbCR.dbo.crtbgest_cred                ||true  |      31|
173|trg_outbox_crtbinfo_gara_real_sibs      |dbCR.dbo.crtbinfo_gara_real_sibs      ||true  |      31|
174|trg_outbox_crtbinfo_legl                |dbCR.dbo.crtbinfo_legl                ||true  |      31|
175|trg_outbox_crtbobli_rol                 |dbCR.dbo.crtbobli_rol                 ||true  |      31|
176|trg_outbox_crtboper_canc                |dbCR.dbo.crtboper_canc                ||true  |      31|
265|trg_outbox_crtboper_cony                |dbCR.dbo.crtboper_cony                ||true  |      20|
177|trg_outbox_crtboper_dref_liqd           |dbCR.dbo.crtboper_dref_liqd           ||true  |      31|
178|trg_outbox_crtbplan_ajus                |dbCR.dbo.crtbplan_ajus                ||true  |      31|
179|trg_outbox_crtbplpg_conv                |dbCR.dbo.crtbplpg_conv                ||true  |      31|
180|trg_outbox_crtbrecu_conv                |dbCR.dbo.crtbrecu_conv                ||true  |      31|
181|trg_outbox_crtbrepo_sobr                |dbCR.dbo.crtbrepo_sobr                ||true  |      31|
182|trg_outbox_crtbrngo_autr_cred           |dbCR.dbo.crtbrngo_autr_cred           ||true  |      31|
183|trg_outbox_crtbrngo_intr_cred           |dbCR.dbo.crtbrngo_intr_cred           ||true  |      31|
184|trg_outbox_crtbsald_cart                |dbCR.dbo.crtbsald_cart                ||true  |      31|
185|trg_outbox_crtbsald_cart_deta           |dbCR.dbo.crtbsald_cart_deta           ||true  |      31|
186|trg_outbox_crtbsegi_autr_ofic           |dbCR.dbo.crtbsegi_autr_ofic           ||true  |      31|
187|trg_outbox_crtbsegu_cred                |dbCR.dbo.crtbsegu_cred                ||true  |      31|
188|trg_outbox_crtbsobr_caut                |dbCR.dbo.crtbsobr_caut                ||true  |      31|
189|trg_outbox_crtbtipo_cred_sibs           |dbCR.dbo.crtbtipo_cred_sibs           ||true  |      31|
338|trg_outbox_crtgrtes                     |dbCR.dbo.crtgrtes                     ||true  |      28|
266|trg_outbox_crtoblig                     |dbCR.dbo.crtoblig                     ||true  |      20|
190|trg_outbox_crtpagos                     |dbCR.dbo.crtpagos                     ||true  |      31|
191|trg_outbox_crtplpag                     |dbCR.dbo.crtplpag                     ||true  |      31|
192|trg_outbox_crtrepo_sobr                 |dbCR.dbo.crtrepo_sobr                 ||true  |      31|
193|trg_outbox_crtrubros_cobr               |dbCR.dbo.crtrubros_cobr               ||true  |      31|
194|trg_outbox_crtsobrante                  |dbCR.dbo.crtsobrante                  ||true  |      31|
195|trg_outbox_crtsolid                     |dbCR.dbo.crtsolid                     ||true  |      31|
142|trg_outbox_cttbafil_audi                |dbCT.dbo.cttbafil_audi                ||true  |      23|
143|trg_outbox_cttbcomi_cred                |dbCT.dbo.cttbcomi_cred                ||true  |      23|
339|trg_outbox_cttbdist_cant                |dbCT.dbo.cttbdist_cant                ||true  |      33|
196|trg_outbox_cttbesta_docu_inve           |dbCT.dbo.cttbesta_docu_inve           ||true  |      31|
144|trg_outbox_cttbmatr_dist_afil           |dbCT.dbo.cttbmatr_dist_afil           ||true  |      23|
197|trg_outbox_cttbproc_obse_tran           |dbCT.dbo.cttbproc_obse_tran           ||true  |      31|
198|trg_outbox_cttbrepo_gene                |dbCT.dbo.cttbrepo_gene                ||true  |      31|
145|trg_outbox_cttbtabl_afil                |dbCT.dbo.cttbtabl_afil                ||true  |      23|
199|trg_outbox_cttbtran_inve_auxi           |dbCT.dbo.cttbtran_inve_auxi           ||true  |      31|
208|trg_outbox_fctbactv_suje_cred           |dbFC.dbo.fctbactv_suje_cred           ||true  |      20|
209|trg_outbox_fctbafil_actu                |dbFC.dbo.fctbafil_actu                ||true  |      20|
210|trg_outbox_fctbafil_ahor_refe           |dbFC.dbo.fctbafil_ahor_refe           ||true  |      20|
211|trg_outbox_fctbafil_auto_docs           |dbFC.dbo.fctbafil_auto_docs           ||true  |      20|
212|trg_outbox_fctbafil_dcap                |dbFC.dbo.fctbafil_dcap                ||true  |      20|
213|trg_outbox_fctbafil_gast_pers           |dbFC.dbo.fctbafil_gast_pers           ||true  |      20|
214|trg_outbox_fctbafil_info_actu_docs      |dbFC.dbo.fctbafil_info_actu_docs      ||true  |      20|
215|trg_outbox_fctbafil_info_adic           |dbFC.dbo.fctbafil_info_adic           ||true  |      20|
216|trg_outbox_fctbafil_unif                |dbFC.dbo.fctbafil_unif                ||true  |      20|
217|trg_outbox_fctbagen_mail                |dbFC.dbo.fctbagen_mail                ||true  |      20|
218|trg_outbox_fctbagen_telf_part           |dbFC.dbo.fctbagen_telf_part           ||true  |      20|
219|trg_outbox_fctbagru_moti_repo           |dbFC.dbo.fctbagru_moti_repo           ||true  |      20|
220|trg_outbox_fctbarea_lbrl                |dbFC.dbo.fctbarea_lbrl                ||true  |      20|
221|trg_outbox_fctbaudi_actu_afil           |dbFC.dbo.fctbaudi_actu_afil           ||true  |      20|
222|trg_outbox_fctbaudi_movi                |dbFC.dbo.fctbaudi_movi                ||true  |      20|
369|trg_outbox_fctbbnco_ospi                |dbFC.dbo.fctbbnco_ospi                ||true  |      33|
223|trg_outbox_fctbcart_rpag                |dbFC.dbo.fctbcart_rpag                ||true  |      20|
363|trg_outbox_fctbcinf_liqd_cnta_sibs      |dbFC.dbo.fctbcinf_liqd_cnta_sibs      ||true  |      32|
224|trg_outbox_fctbcinf_part_sibs           |dbFC.dbo.fctbcinf_part_sibs           ||true  |      20|
367|trg_outbox_fctbcret_volu                |dbFC.dbo.fctbcret_volu                ||true  |      32|
225|trg_outbox_fctbcser_adic                |dbFC.dbo.fctbcser_adic                ||true  |      20|
200|trg_outbox_fctbdeta_liqd_cred           |dbFC.dbo.fctbdeta_liqd_cred           ||true  |      31|
379|trg_outbox_fctbdeta_liqd_grnt           |dbFC.dbo.fctbdeta_liqd_grnt           ||true  |      28|
371|trg_outbox_fctbdeta_rol_dist            |dbFC.dbo.fctbdeta_rol_dist            ||true  |      33|
201|trg_outbox_fctbdinf_liqd_cnta_sibs      |dbFC.dbo.fctbdinf_liqd_cnta_sibs      ||true  |      31|
226|trg_outbox_fctbdinf_part_sibs           |dbFC.dbo.fctbdinf_part_sibs           ||true  |      20|
364|trg_outbox_fctbdret_volu_cred           |dbFC.dbo.fctbdret_volu_cred           ||true  |      32|
365|trg_outbox_fctbdret_volu_dbso           |dbFC.dbo.fctbdret_volu_dbso           ||true  |      32|
358|trg_outbox_fctbdsto_rol                 |dbFC.dbo.fctbdsto_rol                 ||true  |      32|
357|trg_outbox_fctbdsto_rol_audi            |dbFC.dbo.fctbdsto_rol_audi            ||true  |      32|
368|trg_outbox_fctbdsto_serv_gene           |dbFC.dbo.fctbdsto_serv_gene           ||true  |      32|
227|trg_outbox_fctbesta_civi                |dbFC.dbo.fctbesta_civi                ||true  |      20|
228|trg_outbox_fctbfcha_afil                |dbFC.dbo.fctbfcha_afil                ||true  |      20|
229|trg_outbox_fctbfcha_afil_dcto           |dbFC.dbo.fctbfcha_afil_dcto           ||true  |      20|
230|trg_outbox_fctbgene_sibs                |dbFC.dbo.fctbgene_sibs                ||true  |      20|
231|trg_outbox_fctbinst_info_adic           |dbFC.dbo.fctbinst_info_adic           ||true  |      20|
232|trg_outbox_fctbjuri_inst                |dbFC.dbo.fctbjuri_inst                ||true  |      20|
359|trg_outbox_fctbliqd                     |dbFC.dbo.fctbliqd                     ||true  |      32|
360|trg_outbox_fctbliqd_cred                |dbFC.dbo.fctbliqd_cred                ||true  |      32|
361|trg_outbox_fctbliqd_dbso_bene           |dbFC.dbo.fctbliqd_dbso_bene           ||true  |      32|
202|trg_outbox_fctbmvto_impr_esta_cnta      |dbFC.dbo.fctbmvto_impr_esta_cnta      ||true  |      31|
372|trg_outbox_fctbmvto_repo                |dbFC.dbo.fctbmvto_repo                ||true  |      33|
373|trg_outbox_fctborgn_ingr                |dbFC.dbo.fctborgn_ingr                ||true  |      33|
233|trg_outbox_fctbotro_ingr_afil           |dbFC.dbo.fctbotro_ingr_afil           ||true  |      20|
234|trg_outbox_fctbotro_ingr_cony           |dbFC.dbo.fctbotro_ingr_cony           ||true  |      20|
374|trg_outbox_fctbpara                     |dbFC.dbo.fctbpara                     ||true  |      33|
235|trg_outbox_fctbpara_serv_adic           |dbFC.dbo.fctbpara_serv_adic           ||true  |      20|
236|trg_outbox_fctbpart_foto                |dbFC.dbo.fctbpart_foto                ||true  |      20|
203|trg_outbox_fctbproc_tseg_noti           |dbFC.dbo.fctbproc_tseg_noti           ||true  |      31|
366|trg_outbox_fctbreti_volu_para           |dbFC.dbo.fctbreti_volu_para           ||true  |      32|
237|trg_outbox_fctbrubr_rent                |dbFC.dbo.fctbrubr_rent                ||true  |      20|
238|trg_outbox_fctbrvol_esta_afil           |dbFC.dbo.fctbrvol_esta_afil           ||true  |      20|
239|trg_outbox_fctbsald_diar_afil_rubr      |dbFC.dbo.fctbsald_diar_afil_rubr      ||true  |      20|
240|trg_outbox_fctbsald_diar_rubr           |dbFC.dbo.fctbsald_diar_rubr           ||true  |      20|
377|trg_outbox_fctbtdat                     |dbFC.dbo.fctbtdat                     ||true  |      33|
241|trg_outbox_sfct_afiliado                |dbFC.dbo.sfct_afiliado                ||true  |      20|
242|trg_outbox_sfct_afiliado_auditor        |dbFC.dbo.sfct_afiliado_auditor        ||true  |      20|
380|trg_outbox_sfct_afiliado_bienes         |dbFC.dbo.sfct_afiliado_bienes         ||true  |      28|
243|trg_outbox_sfct_afiliado_fondos         |dbFC.dbo.sfct_afiliado_fondos         ||true  |      20|
244|trg_outbox_sfct_afiliado_otros          |dbFC.dbo.sfct_afiliado_otros          ||true  |      20|
204|trg_outbox_sfct_afiliado_referencias    |dbFC.dbo.sfct_afiliado_referencias    ||true  |      31|
245|trg_outbox_sfct_afiliado_rubro          |dbFC.dbo.sfct_afiliado_rubro          ||true  |      20|
246|trg_outbox_sfct_banco                   |dbFC.dbo.sfct_banco                   ||true  |      20|
247|trg_outbox_sfct_beneficiario            |dbFC.dbo.sfct_beneficiario            ||true  |      20|
248|trg_outbox_sfct_beneficiario_retiro     |dbFC.dbo.sfct_beneficiario_retiro     ||true  |      20|
381|trg_outbox_sfct_bienes                  |dbFC.dbo.sfct_bienes                  ||true  |      28|
249|trg_outbox_sfct_cabecera_rol            |dbFC.dbo.sfct_cabecera_rol            ||true  |      20|
370|trg_outbox_sfct_categoria               |dbFC.dbo.sfct_categoria               ||true  |      33|
250|trg_outbox_sfct_ciudad                  |dbFC.dbo.sfct_ciudad                  ||true  |      20|
251|trg_outbox_sfct_conyuge                 |dbFC.dbo.sfct_conyuge                 ||true  |      20|
252|trg_outbox_sfct_detalle_rol             |dbFC.dbo.sfct_detalle_rol             ||true  |      20|
253|trg_outbox_sfct_firmante                |dbFC.dbo.sfct_firmante                ||true  |      20|
254|trg_outbox_sfct_grupo_fami              |dbFC.dbo.sfct_grupo_fami              ||true  |      20|
255|trg_outbox_sfct_institucion             |dbFC.dbo.sfct_institucion             ||true  |      20|
362|trg_outbox_sfct_liqd_hipo               |dbFC.dbo.sfct_liqd_hipo               ||true  |      32|
256|trg_outbox_sfct_motivo                  |dbFC.dbo.sfct_motivo                  ||true  |      20|
257|trg_outbox_sfct_motivo_cnta_cble        |dbFC.dbo.sfct_motivo_cnta_cble        ||true  |      20|
258|trg_outbox_sfct_motivo_mant_afiliados   |dbFC.dbo.sfct_motivo_mant_afiliados   ||true  |      20|
259|trg_outbox_sfct_movimiento              |dbFC.dbo.sfct_movimiento              ||true  |      20|
260|trg_outbox_sfct_movimiento_temp         |dbFC.dbo.sfct_movimiento_temp         ||true  |      20|
261|trg_outbox_sfct_padbs                   |dbFC.dbo.sfct_padbs                   ||true  |      20|
375|trg_outbox_sfct_parroquia               |dbFC.dbo.sfct_parroquia               ||true  |      33|
376|trg_outbox_sfct_provincia               |dbFC.dbo.sfct_provincia               ||true  |      33|
262|trg_outbox_sfct_referencias             |dbFC.dbo.sfct_referencias             ||true  |      20|
263|trg_outbox_sfct_retiro                  |dbFC.dbo.sfct_retiro                  ||true  |      20|
264|trg_outbox_sfct_rubro_rol               |dbFC.dbo.sfct_rubro_rol               ||true  |      20|
205|trg_outbox_sfct_saldos_diarios_afiliados|dbFC.dbo.sfct_saldos_diarios_afiliados||true  |      31|
378|trg_outbox_sfct_titulos                 |dbFC.dbo.sfct_titulos                 ||true  |      33|
382|trg_outbox_gntbdcto                     |dbGN.dbo.gntbdcto                     ||true  |      33|
383|trg_outbox_gntbjorn_lbrl                |dbGN.dbo.gntbjorn_lbrl                ||true  |      33|
384|trg_outbox_gntboper_celu                |dbGN.dbo.gntboper_celu                ||true  |      33|
385|trg_outbox_gntbpais                     |dbGN.dbo.gntbpais                     ||true  |      33|
279|trg_outbox_imtbaben                     |dbIM.dbo.imtbaben                     ||true  |      22|
280|trg_outbox_imtbapro                     |dbIM.dbo.imtbapro                     ||true  |      22|
281|trg_outbox_imtbbene_audi                |dbIM.dbo.imtbbene_audi                ||true  |      22|
282|trg_outbox_imtbbene_entr                |dbIM.dbo.imtbbene_entr                ||true  |      22|
206|trg_outbox_imtbbene_firm                |dbIM.dbo.imtbbene_firm                ||true  |      20|
283|trg_outbox_imtbbloq                     |dbIM.dbo.imtbbloq                     ||true  |      22|
284|trg_outbox_imtbcasi_vivi                |dbIM.dbo.imtbcasi_vivi                ||true  |      22|
285|trg_outbox_imtbcben                     |dbIM.dbo.imtbcben                     ||true  |      22|
286|trg_outbox_imtbcpro                     |dbIM.dbo.imtbcpro                     ||true  |      22|
287|trg_outbox_imtbcpro_dsto                |dbIM.dbo.imtbcpro_dsto                ||true  |      22|
288|trg_outbox_imtbdalt                     |dbIM.dbo.imtbdalt                     ||true  |      22|
289|trg_outbox_imtbdben                     |dbIM.dbo.imtbdben                     ||true  |      22|
290|trg_outbox_imtbdivi                     |dbIM.dbo.imtbdivi                     ||true  |      22|
291|trg_outbox_imtbdpro                     |dbIM.dbo.imtbdpro                     ||true  |      22|
292|trg_outbox_imtbgrcl                     |dbIM.dbo.imtbgrcl                     ||true  |      22|
293|trg_outbox_imtbgrcl_miem                |dbIM.dbo.imtbgrcl_miem                ||true  |      22|
294|trg_outbox_imtbgrcl_tran                |dbIM.dbo.imtbgrcl_tran                ||true  |      22|
295|trg_outbox_imtbgrcl_tran_temp           |dbIM.dbo.imtbgrcl_tran_temp           ||true  |      22|
296|trg_outbox_imtbmiem                     |dbIM.dbo.imtbmiem                     ||true  |      22|
207|trg_outbox_imtbmiem_cony                |dbIM.dbo.imtbmiem_cony                ||true  |      20|
297|trg_outbox_imtbmiem_reti                |dbIM.dbo.imtbmiem_reti                ||true  |      22|
298|trg_outbox_imtbmiem_reti_deta           |dbIM.dbo.imtbmiem_reti_deta           ||true  |      22|
299|trg_outbox_imtbspro                     |dbIM.dbo.imtbspro                     ||true  |      22|
300|trg_outbox_imtbtram                     |dbIM.dbo.imtbtram                     ||true  |      22|
340|trg_outbox_intbcabe_dbso_inve           |dbIN.dbo.intbcabe_dbso_inve           ||true  |      21|
341|trg_outbox_intbccnf_intr_inve           |dbIN.dbo.intbccnf_intr_inve           ||true  |      21|
342|trg_outbox_intbcdvg_inve                |dbIN.dbo.intbcdvg_inve                ||true  |      21|
343|trg_outbox_intbcinv                     |dbIN.dbo.intbcinv                     ||true  |      21|
344|trg_outbox_intbcval_inve                |dbIN.dbo.intbcval_inve                ||true  |      21|
345|trg_outbox_intbdcnf_intr_inve           |dbIN.dbo.intbdcnf_intr_inve           ||true  |      21|
346|trg_outbox_intbdcto                     |dbIN.dbo.intbdcto                     ||true  |      21|
347|trg_outbox_intbdcto_cble                |dbIN.dbo.intbdcto_cble                ||true  |      21|
348|trg_outbox_intbdcto_rubr_cble           |dbIN.dbo.intbdcto_rubr_cble           ||true  |      21|
349|trg_outbox_intbddvg_inve                |dbIN.dbo.intbddvg_inve                ||true  |      21|
350|trg_outbox_intbdinv                     |dbIN.dbo.intbdinv                     ||true  |      21|
351|trg_outbox_intbdval_inve                |dbIN.dbo.intbdval_inve                ||true  |      21|
352|trg_outbox_intbemis                     |dbIN.dbo.intbemis                     ||true  |      21|
353|trg_outbox_intbgara                     |dbIN.dbo.intbgara                     ||true  |      28|
354|trg_outbox_intbprec_diar                |dbIN.dbo.intbprec_diar                ||true  |      21|
355|trg_outbox_intbreaj_inte                |dbIN.dbo.intbreaj_inte                ||true  |      21|
356|trg_outbox_intbvinv                     |dbIN.dbo.intbvinv                     ||true  |      21|
268|trg_outbox_notbcgfm                     |dbNO.dbo.notbcgfm                     ||true  |      20|
269|trg_outbox_notbempl                     |dbNO.dbo.notbempl                     ||true  |      20|
131|trg_outbox_rctbapli_reca                |dbRC.dbo.rctbapli_reca                ||true  |      26|
132|trg_outbox_rctbcaut                     |dbRC.dbo.rctbcaut                     ||true  |      26|
138|trg_outbox_rctbcsal_reca                |dbRC.dbo.rctbcsal_reca                ||true  |      26|
139|trg_outbox_rctbcsci_bce                 |dbRC.dbo.rctbcsci_bce                 ||true  |      26|
386|trg_outbox_rctbcscr                     |dbRC.dbo.rctbcscr                     ||true  |      30|
133|trg_outbox_rctbdevo_rind                |dbRC.dbo.rctbdevo_rind                ||true  |      26|
387|trg_outbox_rctbdscr                     |dbRC.dbo.rctbdscr                     ||true  |      30|
134|trg_outbox_rctbesta_cnta                |dbRC.dbo.rctbesta_cnta                ||true  |      26|
388|trg_outbox_rctbesta_cnta_bloq           |dbRC.dbo.rctbesta_cnta_bloq           ||true  |      32|
135|trg_outbox_rctbesta_reca                |dbRC.dbo.rctbesta_reca                ||true  |      26|
136|trg_outbox_rctbreca                     |dbRC.dbo.rctbreca                     ||true  |      26|
137|trg_outbox_rctbrind                     |dbRC.dbo.rctbrind                     ||true  |      26|
140|trg_outbox_rctbtipo_desc                |dbRC.dbo.rctbtipo_desc                ||true  |      26|
141|trg_outbox_rctbtrec                     |dbRC.dbo.rctbtrec                     ||true  |      26|
120|trg_outbox_sgtbapli                     |dbSG.dbo.sgtbapli                     ||true  |      25|
122|trg_outbox_sgtbcnts                     |dbSG.dbo.sgtbcnts                     ||true  |      25|
130|trg_outbox_sgtbconf_serv_apli           |dbSG.dbo.sgtbconf_serv_apli           ||true  |      25|
123|trg_outbox_sgtbempr                     |dbSG.dbo.sgtbempr                     ||true  |      25|
124|trg_outbox_sgtbfirm                     |dbSG.dbo.sgtbfirm                     ||true  |      25|
125|trg_outbox_sgtbfond                     |dbSG.dbo.sgtbfond                     ||true  |      25|
126|trg_outbox_sgtbloca                     |dbSG.dbo.sgtbloca                     ||true  |      25|
127|trg_outbox_sgtbpara                     |dbSG.dbo.sgtbpara                     ||true  |      25|
128|trg_outbox_sgtbpass                     |dbSG.dbo.sgtbpass                     ||true  |      25|
121|trg_outbox_sgtbtran                     |dbSG.dbo.sgtbtran                     ||true  |      25|
129|trg_outbox_sgtbusua                     |dbSG.dbo.sgtbusua                     ||true  |      25|
270|trg_outbox_svtbcaus                     |dbSV.dbo.svtbcaus                     ||true  |      20|
271|trg_outbox_svtbdisc                     |dbSV.dbo.svtbdisc                     ||true  |      20|
272|trg_outbox_svtbefec                     |dbSV.dbo.svtbefec                     ||true  |      20|
273|trg_outbox_svtbfmpg                     |dbSV.dbo.svtbfmpg                     ||true  |      20|
274|trg_outbox_svtbstro                     |dbSV.dbo.svtbstro                     ||true  |      20|
275|trg_outbox_svtbstro_bene                |dbSV.dbo.svtbstro_bene                ||true  |      20|
276|trg_outbox_svtbstro_cred                |dbSV.dbo.svtbstro_cred                ||true  |      20|
277|trg_outbox_svtbstro_deta                |dbSV.dbo.svtbstro_deta                ||true  |      20|
278|trg_outbox_svtbstro_exte                |dbSV.dbo.svtbstro_exte                ||true  |      20|"""

# Parse: id|name|table|...|active|module|
rows=[]
for line in DATA.strip().split("\n"):
    parts=line.split("|")
    if len(parts) < 6: continue
    try:
        id_=int(parts[0].strip())
        nm=parts[1].strip()
        tbl=parts[2].strip()
        mod=parts[-2].strip()
        rows.append((id_, nm, tbl, mod))
    except:
        pass

print(f"Total filas: {len(rows)}\n")

# Buscar dups por nombre_trigger
by_name=defaultdict(list)
for id_, nm, tbl, mod in rows:
    by_name[nm].append((id_, tbl, mod))

dup_name=[(nm, items) for nm, items in by_name.items() if len(items)>1]
print(f"Triggers DUPLICADOS por nombre_trigger: {len(dup_name)}")
for nm, items in dup_name:
    print(f"  {nm}")
    for id_, tbl, mod in items:
        print(f"    id={id_:>4}  tabla={tbl}  idmodulo={mod}")

# Tambien dup por nombre_tabla
by_tbl=defaultdict(list)
for id_, nm, tbl, mod in rows:
    by_tbl[tbl].append((id_, nm, mod))
dup_tbl=[(tbl, items) for tbl, items in by_tbl.items() if len(items)>1]
print(f"\nNombre_tabla con MULTIPLES triggers registrados: {len(dup_tbl)}")
for tbl, items in dup_tbl:
    print(f"  {tbl}  ({len(items)} entries)")
    for id_, nm, mod in items:
        print(f"    id={id_:>4}  trigger={nm:<40}  idmodulo={mod}")

print(f"\n=== STATS ===")
print(f"  Total entries          : {len(rows)}")
print(f"  Triggers unicos        : {len(by_name)}")
print(f"  Tablas unicas          : {len(by_tbl)}")
print(f"  Triggers DUPLICADOS    : {len(dup_name)}")
print(f"  Tablas con N>1 trigger : {len(dup_tbl)}")
