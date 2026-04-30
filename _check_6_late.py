"""Verifica si los 6 NO_KAFKA ya llegaron a cdc_inbox (despues del test)."""
import sys, pyodbc
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
sys.stdout=Tee(sys.__stdout__, open(r"C:\Users\Usuario\Downloads\Bulk\_check_6_late_out.txt","w",encoding="utf-8"))

DB={'server':'10.35.3.64,1433','driver':'{SQL Server}','username':'sa','password':'YourPassword123'}
c=pyodbc.connect(f"DRIVER={DB['driver']};SERVER={DB['server']};DATABASE=fcme_canonicos;UID={DB['username']};PWD={DB['password']}", autocommit=True, timeout=10).cursor()

LATE=["saldoVinculadoType","seguimientoAutorizacion_type","seguroCredito_type",
      "sobranteCaucion_type","sobranteCredito_type","sobranteDistribucion_type"]

print("Verificando si los 6 NO_KAFKA ya estan en cdc_inbox (ultimos 30 min):\n")
for agg in LATE:
    c.execute("""SELECT COUNT(*) FROM cdc_inbox
                 WHERE aggregate_type=? AND created_at >= DATEADD(MINUTE, -30, SYSDATETIME())""", agg)
    n=c.fetchone()[0]
    flag="LLEGO" if n>0 else "AUN_NO"
    print(f"  [{flag}] {agg:<35} count={n}")

# Total events cartera ultimos 30 min
print("\nTotal eventos cartera en cdc_inbox ultimos 30 min:")
c.execute("""SELECT COUNT(DISTINCT aggregate_type) FROM cdc_inbox
             WHERE source_table LIKE 'FCME_USER.%'
               AND created_at >= DATEADD(MINUTE, -30, SYSDATETIME())""")
print(f"  Types unicos: {c.fetchone()[0]}")
c.execute("""SELECT COUNT(*) FROM cdc_inbox
             WHERE source_table LIKE 'FCME_USER.%'
               AND created_at >= DATEADD(MINUTE, -30, SYSDATETIME())""")
print(f"  Total eventos: {c.fetchone()[0]}")
