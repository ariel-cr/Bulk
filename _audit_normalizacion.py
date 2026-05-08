"""Compara fcme_canonicos.participes.* vs fcme_canonicos_normalizada.participes.*"""
import pytds, time

def conn(host, port, db, user, pwd, retries=8, delay=10):
    for i in range(retries):
        try:
            return pytds.connect(host, database=db, user=user, password=pwd, port=port, autocommit=True, timeout=20)
        except Exception as e:
            print(f'  retry {host}:{port}/{db} {i+1}: {str(e)[:60]}')
            time.sleep(delay)
    raise RuntimeError(f'No connection to {host}:{port}/{db}')

print('Conectando canonicos (10.35.3.64)...')
can = conn('10.35.3.64', 1433, 'fcme_canonicos', 'sa', 'YourPassword123')
print('Conectando normalizada (capa.federada:31433)...')
nor = conn('capa.federada', 31433, 'fcme_canonicos_normalizada', 'sa', 'SqlServer2025!')
c_can = can.cursor()
c_nor = nor.cursor()

# Listar tablas en participes en ambas
c_can.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
tabs_can = set(r[0] for r in c_can.fetchall())
c_nor.execute("SELECT name FROM sys.tables WHERE schema_id=SCHEMA_ID('participes') ORDER BY name")
tabs_nor = set(r[0] for r in c_nor.fetchall())

only_can = sorted(tabs_can - tabs_nor)
only_nor = sorted(tabs_nor - tabs_can)
both     = sorted(tabs_can & tabs_nor)

print(f'\nTablas solo en canonicos:    {len(only_can)}')
for t in only_can: print(f'  {t}')
print(f'\nTablas solo en normalizada:  {len(only_nor)}')
for t in only_nor: print(f'  {t}')
print(f'\nTablas en ambas:             {len(both)}')

print('\n' + '=' * 100)
print(f'{"tabla participes.*":<46} {"canonicos":>12} {"normalizada":>12} {"delta":>10}  status')
print('-' * 100)

results = []
for t in both:
    qt = f'participes.[{t}]'
    a = b = None
    try:
        c_can.execute(f'SELECT COUNT(*) FROM {qt}')
        a = c_can.fetchone()[0]
    except Exception as e:
        a = f'ERR: {str(e)[:40]}'
    try:
        c_nor.execute(f'SELECT COUNT(*) FROM {qt}')
        b = c_nor.fetchone()[0]
    except Exception as e:
        b = f'ERR: {str(e)[:40]}'
    if isinstance(a, int) and isinstance(b, int):
        delta = a - b
        if delta == 0 and a == 0:        status = 'AMBAS VACIAS'
        elif delta == 0:                  status = 'OK iguales'
        elif b == 0:                      status = 'NORMALIZADA SIN DATOS'
        elif delta > 0:                   status = f'falta {delta}'
        else:                             status = f'sobran {-delta}'
    else:
        delta = '?'
        status = 'error'
    print(f'{t:<46} {str(a):>12} {str(b):>12} {str(delta):>10}  {status}')
    results.append((t, a, b, delta, status))

print('\n' + '=' * 100)
print('RESUMEN')
print('=' * 100)
ok = [r for r in results if r[4]=='OK iguales']
empty = [r for r in results if r[4]=='AMBAS VACIAS']
no_data = [r for r in results if r[4]=='NORMALIZADA SIN DATOS']
falta = [r for r in results if r[4].startswith('falta')]
sobran = [r for r in results if r[4].startswith('sobran')]
errores = [r for r in results if r[4]=='error']

print(f'  OK (counts iguales con datos):       {len(ok)}')
print(f'  Ambas tablas vacias:                 {len(empty)}')
print(f'  Normalizada vacia (canonicos tiene): {len(no_data)}')
print(f'  Faltan filas en normalizada:         {len(falta)}')
print(f'  Sobran filas en normalizada:         {len(sobran)}')
print(f'  Errores de query:                    {len(errores)}')

if no_data:
    print('\n[!] NORMALIZADA SIN DATOS (pero canonicos tiene):')
    for r in no_data: print(f'  {r[0]:<46} canonicos={r[1]}')

if falta:
    print('\n[!] FALTAN FILAS EN NORMALIZADA:')
    for r in sorted(falta, key=lambda x: -x[3])[:30]:
        print(f'  {r[0]:<46} canonicos={r[1]:<8} normalizada={r[2]:<8} faltan={r[3]}')

can.close()
nor.close()
