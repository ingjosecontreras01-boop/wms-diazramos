from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import sqlite3, pandas as pd, io, os, traceback
from datetime import datetime

app = Flask(__name__, template_folder='.', static_folder='.', static_url_path='/static')
FOTOS_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fotos_salida')
os.makedirs(FOTOS_DIR, exist_ok=True)
DB = 'bodega.db'
RACKS = ['J', 'K', 'L', 'M']

ESTRUCTURA_RACKS = {
    'J': {
        'N1': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N2': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N3': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N4': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N5': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N6': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N7': ['01','02','03','04','05','06','07','08'],
    },
    'K': {
        'N1': ['01','02','03','04','07','08','09','10','11','12'],
        'N2': ['01','02','03','04','07','08','09','10','11','12'],
        'N3': ['01','02','03','04','07','08','09','10','11','12'],
        'N4': ['01','02','03','04','07','08','09','10','11','12'],
        'N5': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N6': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N7': ['01','02','03','04','05','06','07','08'],
    },
    'L': {
        'N1': ['01','02','03','04','07','08','09','10','11','12'],
        'N2': ['01','02','03','04','07','08','09','10','11','12'],
        'N3': ['01','02','03','04','07','08','09','10','11','12'],
        'N4': ['01','02','03','04','07','08','09','10','11','12'],
        'N5': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N6': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N7': ['01','02','03','04','05','06','07','08'],
    },
    'M': {
        'N1': ['01','02','03','04','07','08','09','10','11','12'],
        'N2': ['01','02','03','04','07','08','09','10','11','12'],
        'N3': ['01','02','03','04','07','08','09','10','11','12'],
        'N4': ['01','02','03','04','07','08','09','10','11','12'],
        'N5': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N6': ['01','02','03','04','05','06','07','08','09','10','11','12'],
        'N7': ['01','02','03','04','05','06','07','08'],
    },
}

COLORES = ['#0782c2','#00b140','#fe5000','#9c27b0','#ff9800','#e91e63',
           '#009688','#795548','#607d8b','#3f51b5','#8bc34a','#f44336']

def get_db():
    conn = sqlite3.connect(DB)
    conn.row_factory = sqlite3.Row
    return conn

def init_db():
    conn = get_db()
    c = conn.cursor()
    c.execute('''CREATE TABLE IF NOT EXISTS referencias (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        nombre TEXT NOT NULL UNIQUE,
        grupo TEXT DEFAULT '',
        marca TEXT DEFAULT '',
        referencia_articulo TEXT DEFAULT '',
        peso_unidad REAL DEFAULT 0,
        unidades_por_estiba INTEGER NOT NULL DEFAULT 1,
        color TEXT DEFAULT '#0782c2'
    )''')
    # Migración: agregar columnas si no existen (para BD existentes)
    for col in [('grupo','TEXT','""'), ('marca','TEXT','""'), ('referencia_articulo','TEXT','""')]:
        try:
            c.execute(f"ALTER TABLE referencias ADD COLUMN {col[0]} {col[1]} DEFAULT {col[2]}")
        except: pass
    c.execute('''CREATE TABLE IF NOT EXISTS posiciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rack TEXT NOT NULL, nivel TEXT NOT NULL, posicion TEXT NOT NULL,
        activa INTEGER DEFAULT 1, UNIQUE(rack, nivel, posicion)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS inventario (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        posicion_id INTEGER NOT NULL, referencia_id INTEGER NOT NULL,
        lote TEXT DEFAULT '', fecha_entrada TEXT NOT NULL,
        fecha_vencimiento TEXT DEFAULT '',
        unidades INTEGER DEFAULT 0,
        actualizado_erp INTEGER DEFAULT 0,
        FOREIGN KEY (posicion_id) REFERENCES posiciones(id),
        FOREIGN KEY (referencia_id) REFERENCES referencias(id)
    )''')
    # Migración para BD existentes
    for col in [('fecha_vencimiento','TEXT','""'), ('unidades','INTEGER','0')]:
        try:
            c.execute(f"ALTER TABLE inventario ADD COLUMN {col[0]} {col[1]} DEFAULT {col[2]}")
        except: pass
    # Quitar UNIQUE de posicion_id si existe (migración)
    try:
        c.execute('''CREATE TABLE IF NOT EXISTS inventario_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            posicion_id INTEGER NOT NULL, referencia_id INTEGER NOT NULL,
            lote TEXT DEFAULT '', fecha_entrada TEXT NOT NULL,
            fecha_vencimiento TEXT DEFAULT '',
            unidades INTEGER DEFAULT 0,
            actualizado_erp INTEGER DEFAULT 0
        )''')
        # Solo migrar si inventario_new está vacía y inventario tiene datos
        count_new = c.execute('SELECT COUNT(*) FROM inventario_new').fetchone()[0]
        count_old = c.execute('SELECT COUNT(*) FROM inventario').fetchone()[0]
        if count_new == 0 and count_old > 0:
            c.execute('''INSERT INTO inventario_new
                SELECT id, posicion_id, referencia_id, lote, fecha_entrada,
                       COALESCE(fecha_vencimiento,''), COALESCE(unidades,0), actualizado_erp
                FROM inventario''')
            c.execute('DROP TABLE inventario')
            c.execute('ALTER TABLE inventario_new RENAME TO inventario')
    except: pass
    c.execute('''CREATE TABLE IF NOT EXISTS movimientos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL, referencia_id INTEGER, lote TEXT,
        unidades INTEGER DEFAULT 0, estibas INTEGER DEFAULT 0,
        racks TEXT DEFAULT '', fecha TEXT NOT NULL, notas TEXT DEFAULT '',
        operador TEXT DEFAULT ''
    )''')
    # Migraciones para BD existentes
    try: c.execute("ALTER TABLE movimientos ADD COLUMN operador TEXT DEFAULT ''")
    except: pass
    try: c.execute("ALTER TABLE inventario ADD COLUMN operador TEXT DEFAULT ''")
    except: pass
    # Migración: corregir unidades_por_estiba = 0 (causaba división por cero)
    try:
        c.execute("UPDATE referencias SET unidades_por_estiba = 1 WHERE unidades_por_estiba IS NULL OR unidades_por_estiba <= 0")
    except: pass
    for rack, niveles in ESTRUCTURA_RACKS.items():
        for nivel, posiciones in niveles.items():
            for pos in posiciones:
                c.execute('INSERT OR IGNORE INTO posiciones (rack,nivel,posicion,activa) VALUES (?,?,?,1)',
                          (rack, nivel, pos))
    conn.commit()
    conn.close()

init_db()

@app.route('/fotos_salida/<path:filename>')
def servir_foto(filename):
    return send_from_directory(FOTOS_DIR, filename)

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/movil')
def movil():
    return render_template('movil.html')

@app.route('/escritorio')
def escritorio():
    return render_template('escritorio.html')

@app.route('/api/referencias', methods=['GET'])
def get_referencias():
    conn = get_db()
    q = request.args.get('q', '').strip()
    if q:
        refs = conn.execute('''SELECT * FROM referencias
            WHERE nombre LIKE ? OR grupo LIKE ? OR marca LIKE ? OR referencia_articulo LIKE ?
            ORDER BY grupo, nombre''',
            (f'%{q}%', f'%{q}%', f'%{q}%', f'%{q}%')).fetchall()
    else:
        refs = conn.execute('SELECT * FROM referencias ORDER BY grupo, nombre').fetchall()
    conn.close()
    return jsonify([dict(r) for r in refs])

@app.route('/api/upload_referencias', methods=['POST'])
def upload_referencias():
    if 'file' not in request.files:
        return jsonify({'error': 'Sin archivo'}), 400
    f = request.files['file']
    try:
        df = pd.read_excel(f)
        conn = get_db()
        count = 0
        # Mapa de colores por grupo (consistente entre cargas)
        grupos_colores = {}
        grupos_existentes = conn.execute('SELECT DISTINCT grupo, color FROM referencias WHERE grupo != ""').fetchall()
        for g in grupos_existentes:
            grupos_colores[g['grupo']] = g['color']

        for i, row in df.iterrows():
            # Columnas: A=Grupo, B=Marca, C=Referencia, D=SKU(nombre), E=UxEstiba, F=Peso
            grupo = str(row.iloc[0]).strip() if len(row) > 0 else ''
            marca = str(row.iloc[1]).strip() if len(row) > 1 else ''
            ref_art = str(row.iloc[2]).strip() if len(row) > 2 else ''
            nombre = str(row.iloc[3]).strip() if len(row) > 3 else ''  # SKU = nombre principal

            if not nombre or nombre == 'nan': continue
            if grupo == 'nan': grupo = ''
            if marca == 'nan': marca = ''
            if ref_art == 'nan': ref_art = ''

            # UxEstiba columna E, Peso columna F
            upE_val = row.iloc[4] if len(row) > 4 else None
            peso_val = row.iloc[5] if len(row) > 5 else None
            try:
                upE = int(float(upE_val)) if upE_val is not None and str(upE_val) not in ('nan','#N/D','#N/A') else 1
            except: upE = 1
            if upE <= 0: upE = 1
            try:
                peso = float(peso_val) if peso_val is not None and str(peso_val) not in ('nan','#N/D','#N/A') else 0
            except: peso = 0

            # Asignar color por grupo
            if grupo and grupo in grupos_colores:
                color = grupos_colores[grupo]
            else:
                color = COLORES[len(grupos_colores) % len(COLORES)]
                if grupo:
                    grupos_colores[grupo] = color

            try:
                conn.execute('''INSERT OR IGNORE INTO referencias
                    (nombre,grupo,marca,referencia_articulo,peso_unidad,unidades_por_estiba,color)
                    VALUES (?,?,?,?,?,?,?)''',
                    (nombre, grupo, marca, ref_art, peso, upE, color))
                count += 1
            except: pass

        conn.commit()
        conn.close()
        return jsonify({'ok': True, 'cargadas': count})
    except Exception as e:
        return jsonify({'error': str(e)}), 500

@app.route('/api/posiciones_libres')
def posiciones_libres():
    rack = request.args.get('rack', '')
    conn = get_db()
    q = '''SELECT p.id, p.rack, p.nivel, p.posicion FROM posiciones p
           WHERE p.activa=1 AND p.id NOT IN (SELECT posicion_id FROM inventario)'''
    params = []
    if rack:
        q += ' AND p.rack=?'
        params.append(rack)
    q += ' ORDER BY p.rack, p.nivel, p.posicion'
    pos = conn.execute(q, params).fetchall()
    conn.close()
    por_rack = {}
    for p in pos:
        por_rack[p['rack']] = por_rack.get(p['rack'], 0) + 1
    return jsonify({'total': len(pos), 'por_rack': por_rack, 'posiciones': [dict(p) for p in pos]})

@app.route('/api/estructura_racks')
def estructura_racks_api():
    return jsonify(ESTRUCTURA_RACKS)

@app.route('/api/registrar_entrada_v2', methods=['POST'])
def registrar_entrada_v2():
    data = request.json
    ref_id = data.get('referencia_id')
    unidades = int(data.get('unidades', 0))
    estibas = int(data.get('estibas', 0))
    rack = data.get('rack', '')
    posiciones_estibas = data.get('posiciones_estibas', [])
    operador = data.get('operador', '')
    if not posiciones_estibas:
        return jsonify({'error': 'Sin posiciones'}), 400
    conn = get_db()
    fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    registradas = 0
    advertencias = []
    for pe in posiciones_estibas:
        pos_id = pe.get('posicion_id')
        lotes = pe.get('lotes', [])
        def fmt_lote(l):
            s = f"{l['lote']}({l.get('cantidad',0)}u)"
            if l.get('fecha_vencimiento'): s += f"[v:{l['fecha_vencimiento']}]"
            return s
        lote_str = ' | '.join([fmt_lote(l) for l in lotes if l.get('lote')]) if lotes else ''
        # fecha_vencimiento de la estiba = la del primer lote que la tenga
        fvenc = next((l.get('fecha_vencimiento','') for l in lotes if l.get('fecha_vencimiento')), pe.get('fecha_vencimiento', ''))
        unis = pe.get('unidades', 0)

        # Si unis es 0 o None, usar unidades_por_estiba de la referencia como fallback
        if not unis or unis <= 0:
            ref_fallback = conn.execute('SELECT unidades_por_estiba FROM referencias WHERE id=?', (ref_id,)).fetchone()
            unis = ref_fallback['unidades_por_estiba'] if ref_fallback else 1

        # Verificar peso acumulado en la posición
        peso_actual = conn.execute('''
            SELECT COALESCE(SUM(
                CASE WHEN i.unidades > 0 THEN i.unidades * r.peso_unidad
                     ELSE r.unidades_por_estiba * r.peso_unidad END
            ), 0)
            FROM inventario i JOIN referencias r ON i.referencia_id=r.id
            WHERE i.posicion_id=?
        ''', (pos_id,)).fetchone()[0]
        # Peso nuevo
        ref_info = conn.execute('SELECT peso_unidad, unidades_por_estiba FROM referencias WHERE id=?', (ref_id,)).fetchone()
        peso_nuevo = (unis or (ref_info['unidades_por_estiba'] if ref_info else 1)) * (ref_info['peso_unidad'] if ref_info else 0)
        if peso_actual + peso_nuevo > 1000:
            advertencias.append(f'Posición {pos_id}: excede 1000kg ({round(peso_actual+peso_nuevo,1)}kg)')

        conn.execute('''INSERT INTO inventario
            (posicion_id, referencia_id, lote, fecha_entrada, fecha_vencimiento, unidades, operador)
            VALUES (?,?,?,?,?,?,?)''',
            (pos_id, ref_id, lote_str, fecha, fvenc, unis, operador))
        registradas += 1

    conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,operador) VALUES (?,?,?,?,?,?,?,?)',
                 ('entrada', ref_id, '', unidades, estibas, rack, fecha, operador))
    conn.commit()
    conn.close()
    resp = {'ok': True, 'registradas': registradas}
    if advertencias:
        resp['advertencia'] = ' | '.join(advertencias)
    return jsonify(resp)

@app.route('/api/estado_racks')
def estado_racks():
    conn = get_db()
    # Traer todas las posiciones
    posiciones = conn.execute('''
        SELECT p.id, p.rack, p.nivel, p.posicion, p.activa
        FROM posiciones p ORDER BY p.rack, p.nivel, p.posicion
    ''').fetchall()
    # Traer inventario con peso
    inventario = conn.execute('''
        SELECT i.id as inv_id, i.posicion_id, i.lote, i.fecha_entrada,
               i.fecha_vencimiento, i.unidades, i.actualizado_erp,
               r.nombre as referencia, r.id as ref_id, r.color,
               r.peso_unidad, r.unidades_por_estiba, r.grupo, r.marca,
               COALESCE(i.operador,'') as operador
        FROM inventario i
        JOIN referencias r ON i.referencia_id = r.id
    ''').fetchall()
    conn.close()

    # Agrupar inventario por posicion_id
    inv_por_pos = {}
    for i in inventario:
        pid = i['posicion_id']
        if pid not in inv_por_pos:
            inv_por_pos[pid] = []
        inv_por_pos[pid].append(dict(i))

    result = []
    for p in posiciones:
        row = dict(p)
        items = inv_por_pos.get(p['id'], [])
        if items:
            row['inv_id'] = items[0]['inv_id']  # compatibilidad
            row['lote'] = items[0]['lote']
            row['fecha_entrada'] = items[0]['fecha_entrada']
            row['actualizado_erp'] = all(i['actualizado_erp'] for i in items)
            row['referencia'] = items[0]['referencia']
            row['ref_id'] = items[0]['ref_id']
            row['color'] = items[0]['color']
            row['grupo'] = items[0].get('grupo','')
            row['marca'] = items[0].get('marca','')
            row['operador'] = items[0].get('operador','')
            row['unidades'] = items[0].get('unidades', 0)
            row['unidades_por_estiba'] = items[0].get('unidades_por_estiba', 1)
            row['peso_unidad'] = items[0].get('peso_unidad', 0)
            row['items'] = items  # todos los productos
            # Peso total acumulado
            peso_total = sum(
                (i.get('unidades') if i.get('unidades') and i.get('unidades') > 0 else i.get('unidades_por_estiba', 1)) * i.get('peso_unidad', 0)
                for i in items
            )
            row['peso_total'] = round(peso_total, 1)
            row['multi'] = len(items) > 1
        else:
            row['inv_id'] = None
            row['items'] = []
            row['peso_total'] = 0
            row['multi'] = False
        result.append(row)
    return jsonify(result)

@app.route('/api/posicion_info/<int:pos_id>')
def posicion_info(pos_id):
    conn = get_db()
    items = conn.execute('''
        SELECT i.id, i.lote, i.fecha_entrada, i.fecha_vencimiento,
               i.unidades, i.actualizado_erp,
               r.nombre as referencia, r.color, r.peso_unidad,
               r.unidades_por_estiba, r.grupo,
               COALESCE(i.operador,'') as operador
        FROM inventario i JOIN referencias r ON i.referencia_id=r.id
        WHERE i.posicion_id=?
    ''', (pos_id,)).fetchall()
    peso_total = sum((i['unidades'] if i['unidades'] and i['unidades'] > 0 else i['unidades_por_estiba']) * i['peso_unidad'] for i in items)
    conn.close()
    return jsonify({
        'items': [dict(i) for i in items],
        'peso_total': round(peso_total, 1),
        'capacidad': 1000,
        'disponible': round(1000 - peso_total, 1)
    })

@app.route('/api/marcar_erp/<int:inv_id>', methods=['POST'])
def marcar_erp(inv_id):
    conn = get_db()
    conn.execute('UPDATE inventario SET actualizado_erp=1 WHERE id=?', (inv_id,))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})

@app.route('/api/buscar_lote')
def buscar_lote():
    lote = request.args.get('q', '')
    conn = get_db()
    # Stock actual
    rows = conn.execute('''
        SELECT i.id, i.lote, i.fecha_entrada, i.actualizado_erp,
               p.rack, p.nivel, p.posicion, r.nombre as referencia, r.color,
               'stock' as origen, '' as foto, '' as fecha_mov, '' as tipo_mov
        FROM inventario i JOIN posiciones p ON i.posicion_id = p.id
        JOIN referencias r ON i.referencia_id = r.id
        WHERE i.lote LIKE ? ORDER BY i.fecha_entrada DESC
    ''', (f'%{lote}%',)).fetchall()
    # Movimientos con ese lote (salidas)
    try: conn.execute("ALTER TABLE movimientos ADD COLUMN foto TEXT DEFAULT ''")
    except: pass
    movs = conn.execute('''
        SELECT m.id, m.lote, m.fecha as fecha_entrada, 0 as actualizado_erp,
               '' as rack, '' as nivel, '' as posicion,
               r.nombre as referencia, r.color,
               'movimiento' as origen,
               COALESCE(m.foto,'') as foto,
               m.fecha as fecha_mov, m.tipo as tipo_mov,
               m.racks, m.unidades, COALESCE(m.operador,'') as operador
        FROM movimientos m
        LEFT JOIN referencias r ON m.referencia_id = r.id
        WHERE m.lote LIKE ? AND m.tipo IN ('salida','salida_parcial')
        ORDER BY m.fecha DESC
    ''', (f'%{lote}%',)).fetchall()
    conn.close()
    resultado = [dict(r) for r in rows] + [dict(m) for m in movs]
    return jsonify(resultado)

@app.route('/api/trazabilidad_lote')
def trazabilidad_lote():
    q = request.args.get('q', '').strip()
    if not q:
        return jsonify({'stock': [], 'movimientos': []})
    conn = get_db()

    # Buscar en inventario actual (aún en bodega)
    stock = conn.execute('''
        SELECT i.id, i.lote, i.fecha_entrada, i.fecha_vencimiento,
               i.unidades, i.actualizado_erp,
               COALESCE(i.operador,'') as operador,
               p.rack||'-'||p.nivel||'-'||p.posicion as ubicacion,
               r.nombre as referencia, r.grupo, r.marca,
               r.referencia_articulo, r.color, r.peso_unidad,
               r.unidades_por_estiba
        FROM inventario i
        JOIN posiciones p ON i.posicion_id = p.id
        JOIN referencias r ON i.referencia_id = r.id
        WHERE i.lote LIKE ?
        ORDER BY i.fecha_entrada DESC
    ''', (f'%{q}%',)).fetchall()

    # Buscar en movimientos (salidas, traslados, entradas registradas)
    try: conn.execute("ALTER TABLE movimientos ADD COLUMN foto TEXT DEFAULT ''")
    except: pass
    conn.commit()
    movs = conn.execute('''
        SELECT m.id, m.tipo, m.lote, m.unidades, m.estibas,
               m.racks, m.fecha, m.notas,
               COALESCE(m.operador,'') as operador,
               COALESCE(m.foto,'') as foto,
               r.nombre as referencia, r.grupo, r.marca,
               r.referencia_articulo, r.color
        FROM movimientos m
        LEFT JOIN referencias r ON m.referencia_id = r.id
        WHERE m.lote LIKE ?
        ORDER BY m.fecha DESC
    ''', (f'%{q}%',)).fetchall()

    conn.close()
    return jsonify({
        'stock': [dict(r) for r in stock],
        'movimientos': [dict(m) for m in movs]
    })

@app.route('/api/stats')
def stats():
    conn = get_db()
    total = conn.execute('SELECT COUNT(*) FROM posiciones WHERE activa=1').fetchone()[0]
    ocupadas = conn.execute('SELECT COUNT(*) FROM inventario').fetchone()[0]
    pendientes_erp = conn.execute('SELECT COUNT(*) FROM inventario WHERE actualizado_erp=0').fetchone()[0]
    por_rack = []
    for rack in RACKS:
        t = conn.execute('SELECT COUNT(*) FROM posiciones WHERE rack=? AND activa=1', (rack,)).fetchone()[0]
        o = conn.execute('SELECT COUNT(*) FROM inventario i JOIN posiciones p ON i.posicion_id=p.id WHERE p.rack=?', (rack,)).fetchone()[0]
        por_rack.append({'rack': rack, 'total': t, 'ocupadas': o, 'libres': t-o, 'pct': round(o/t*100) if t else 0})
    conn.close()
    return jsonify({'total': total, 'ocupadas': ocupadas, 'libres': total-ocupadas,
                    'pendientes_erp': pendientes_erp,
                    'pct_ocupacion': round(ocupadas/total*100) if total else 0,
                    'por_rack': por_rack})

@app.route('/api/exportar_excel')
def exportar_excel():
    conn = get_db()
    rows = conn.execute('''
        SELECT p.rack as "Rack", p.nivel as "Nivel", p.posicion as "Posición",
               r.nombre as "Referencia", i.lote as "Lote",
               r.unidades_por_estiba as "Unidades x Estiba",
               i.fecha_entrada as "Fecha entrada",
               CASE WHEN i.actualizado_erp=1 THEN "Sí" ELSE "No" END as "Actualizado ERP"
        FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
        JOIN referencias r ON i.referencia_id=r.id
        ORDER BY p.rack, p.nivel, p.posicion
    ''').fetchall()
    conn.close()
    df = pd.DataFrame([dict(r) for r in rows])
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Inventario')
    out.seek(0)
    fname = f'inventario_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, download_name=fname)

@app.route('/api/registrar_salida', methods=['POST'])
def registrar_salida():
    import base64, uuid as _uuid
    # Soporta tanto JSON como multipart (con foto)
    if request.content_type and 'multipart' in request.content_type:
        inv_id   = request.form.get('inv_id', type=int)
        tipo     = request.form.get('tipo', 'total')
        unidades = request.form.get('unidades', type=int)
        nota     = request.form.get('nota', '')
        operador = request.form.get('operador', '')
        foto_file = request.files.get('foto')
        foto_nombre = None
        if foto_file and foto_file.filename:
            os.makedirs(FOTOS_DIR, exist_ok=True)
            ext = foto_file.filename.rsplit('.', 1)[-1].lower() if '.' in foto_file.filename else 'jpg'
            foto_nombre = f"sal_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_uuid.uuid4().hex[:6]}.{ext}"
            foto_file.save(os.path.join(FOTOS_DIR, foto_nombre))
    else:
        data     = request.json or {}
        inv_id   = data.get('inv_id')
        tipo     = data.get('tipo', 'total')
        unidades = data.get('unidades')
        nota     = data.get('nota', '')
        operador = data.get('operador', '')
        foto_nombre = None
        # Soporte foto en base64 (fallback)
        foto_b64 = data.get('foto_base64')
        if foto_b64:
            try:
                os.makedirs(FOTOS_DIR, exist_ok=True)
                header, b64data = foto_b64.split(',', 1) if ',' in foto_b64 else ('', foto_b64)
                ext = 'jpg' if 'jpeg' in header else ('png' if 'png' in header else 'jpg')
                foto_nombre = f"sal_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{_uuid.uuid4().hex[:6]}.{ext}"
                with open(os.path.join(FOTOS_DIR, foto_nombre), 'wb') as fimg:
                    fimg.write(base64.b64decode(b64data))
            except: foto_nombre = None

    if not inv_id:
        return jsonify({'error': 'Faltan parámetros'}), 400
    conn = get_db()
    # Migración: agregar columna foto si no existe
    try: conn.execute("ALTER TABLE movimientos ADD COLUMN foto TEXT DEFAULT ''")
    except: pass
    conn.commit()
    inv = conn.execute('''SELECT i.*, p.rack, p.nivel, p.posicion, r.nombre as ref_nombre, r.id as ref_id
                          FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
                          JOIN referencias r ON i.referencia_id=r.id WHERE i.id=?''', (inv_id,)).fetchone()
    if not inv:
        conn.close()
        return jsonify({'error': 'Inventario no encontrado'}), 404
    fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    codigo = f"{inv['rack']}-{inv['nivel']}-{inv['posicion']}"
    if tipo == 'total':
        conn.execute('DELETE FROM inventario WHERE id=?', (inv_id,))
        conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,notas,operador,foto) VALUES (?,?,?,?,?,?,?,?,?,?)',
                     ('salida', inv['ref_id'], inv['lote'], inv['unidades'] or 0, 1, codigo, fecha, nota or 'Salida total', operador, foto_nombre or ''))
    else:
        unidades_actuales = inv['unidades'] or 0
        unidades_salen = unidades or 0
        nuevas_unidades = unidades_actuales - unidades_salen
        conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,notas,operador,foto) VALUES (?,?,?,?,?,?,?,?,?,?)',
                     ('salida_parcial', inv['ref_id'], inv['lote'], unidades_salen, 0, codigo, fecha,
                      nota or f'Salida parcial: {unidades_salen} u', operador, foto_nombre or ''))
        if nuevas_unidades <= 0:
            conn.execute('DELETE FROM inventario WHERE id=?', (inv_id,))
        else:
            conn.execute('UPDATE inventario SET unidades=? WHERE id=?', (nuevas_unidades, inv_id))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'foto': foto_nombre or ''})

@app.route('/api/mover_posicion', methods=['POST'])
def mover_posicion():
    data = request.json
    inv_id = data.get('inv_id')
    nueva_posicion_id = data.get('nueva_posicion_id')
    if not inv_id or not nueva_posicion_id:
        return jsonify({'error': 'Faltan parámetros'}), 400
    conn = get_db()
    ocupada = conn.execute('SELECT id FROM inventario WHERE posicion_id=?', (nueva_posicion_id,)).fetchone()
    if ocupada:
        conn.close()
        return jsonify({'error': 'La posición destino ya está ocupada'}), 400
    inv = conn.execute('''SELECT i.*, p.rack, p.nivel, p.posicion, r.nombre as ref_nombre
                          FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
                          JOIN referencias r ON i.referencia_id=r.id WHERE i.id=?''', (inv_id,)).fetchone()
    nueva_pos = conn.execute('SELECT * FROM posiciones WHERE id=?', (nueva_posicion_id,)).fetchone()
    if not inv or not nueva_pos:
        conn.close()
        return jsonify({'error': 'No encontrado'}), 404
    fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    operador = data.get('operador', '')
    origen = f"{inv['rack']}-{inv['nivel']}-{inv['posicion']}"
    destino = f"{nueva_pos['rack']}-{nueva_pos['nivel']}-{nueva_pos['posicion']}"
    conn.execute('UPDATE inventario SET posicion_id=?, actualizado_erp=0 WHERE id=?', (nueva_posicion_id, inv_id))
    conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,notas,operador) VALUES (?,?,?,?,?,?,?,?,?)',
                 ('traslado', inv['referencia_id'], inv['lote'], inv['unidades'] or 0, 1, f"{origen} → {destino}", fecha, 'Traslado', operador))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'nueva_ubicacion': destino, 'origen': origen})

@app.route('/kardex')
def kardex():
    return send_from_directory('.', 'kardex.html')

@app.route('/api/alertas')
def api_alertas():
    import calendar
    conn = get_db()
    hoy = datetime.now()

    # ── VENCIMIENTOS: solo filas con fecha_vencimiento definida ──
    rows_venc = conn.execute('''
        SELECT i.lote, i.fecha_vencimiento, COALESCE(i.unidades,0) as unidades,
               r.nombre as referencia, r.color,
               p.rack||'-'||p.nivel||'-'||p.posicion as ubicacion
        FROM inventario i
        JOIN referencias r ON i.referencia_id = r.id
        JOIN posiciones p ON i.posicion_id = p.id
        WHERE i.fecha_vencimiento IS NOT NULL AND TRIM(i.fecha_vencimiento) != ''
    ''').fetchall()

    vencimientos = []
    for row in rows_venc:
        fv = (row['fecha_vencimiento'] or '').strip()
        try:
            if '/' in fv:
                p2 = fv.split('/')
                mes, anio = int(p2[0]), int(p2[1])
                if anio < 100: anio += 2000
                fecha_venc = datetime(anio, mes, calendar.monthrange(anio, mes)[1])
            elif '-' in fv:
                fecha_venc = datetime.strptime(fv[:10], '%Y-%m-%d')
            else:
                continue
        except:
            continue
        delta = (fecha_venc - hoy).days
        if delta <= 90:
            nivel = 'vencido' if delta < 0 else ('critico' if delta <= 30 else 'proximo')
            vencimientos.append({
                'referencia': row['referencia'], 'color': row['color'],
                'ubicacion': row['ubicacion'], 'fecha_vencimiento': fv,
                'dias': delta, 'nivel': nivel, 'unidades': row['unidades'],
                'lote': (row['lote'] or '').replace('|', '·')
            })
    vencimientos.sort(key=lambda x: x['dias'])

    # ── ROTACIÓN: JOIN eficiente en lugar de subquery correlacionado ──
    rot_rows = conn.execute('''
        SELECT r.nombre as referencia, r.color,
               p.rack||'-'||p.nivel||'-'||p.posicion as ubicacion,
               COALESCE(i.unidades,0) as unidades,
               COALESCE(i.fecha_entrada,'1900-01-01') as ultima_entrada,
               COALESCE(mm.ultimo_mov,'1900-01-01') as ultimo_mov
        FROM inventario i
        JOIN referencias r ON i.referencia_id = r.id
        JOIN posiciones p ON i.posicion_id = p.id
        LEFT JOIN (
            SELECT referencia_id, MAX(fecha) as ultimo_mov
            FROM movimientos GROUP BY referencia_id
        ) mm ON mm.referencia_id = r.id
    ''').fetchall()

    # Deduplicar por referencia tomando la más antigua
    refs_rot = {}
    for row in rot_rows:
        ultima = max(row['ultima_entrada'], row['ultimo_mov'])
        if ultima == '1900-01-01': continue
        try:
            fu = datetime.strptime(ultima[:19], '%Y-%m-%d %H:%M:%S')
        except:
            try: fu = datetime.strptime(ultima[:10], '%Y-%m-%d')
            except: continue
        dias = (hoy - fu).days
        if dias < 180: continue
        ref = row['referencia']
        if ref not in refs_rot or dias > refs_rot[ref]['dias_quieto']:
            refs_rot[ref] = {
                'referencia': ref, 'color': row['color'],
                'ubicacion': row['ubicacion'], 'dias_quieto': dias,
                'ultima_actividad': ultima[:10], 'unidades': row['unidades'],
                'nivel': 'critico' if dias >= 365 else 'advertencia'
            }
    sin_rotacion = sorted(refs_rot.values(), key=lambda x: -x['dias_quieto'])

    conn.close()
    return jsonify({
        'vencimientos': vencimientos,
        'sin_rotacion': sin_rotacion,
        'resumen': {
            'vencidos':      sum(1 for v in vencimientos if v['nivel'] == 'vencido'),
            'criticos':      sum(1 for v in vencimientos if v['nivel'] == 'critico'),
            'proximos':      sum(1 for v in vencimientos if v['nivel'] == 'proximo'),
            'sin_rotacion':  len(sin_rotacion)
        }
    })

@app.route('/api/kardex_export')
def kardex_export():
    conn = get_db()
    ref_id      = request.args.get('ref_id', '')
    tipo        = request.args.get('tipo', '')
    operador    = request.args.get('operador', '')
    fecha_desde = request.args.get('desde', '')
    fecha_hasta = request.args.get('hasta', '')

    cond_m, params_m = ['1=1'], []
    if ref_id:   cond_m.append('m.referencia_id=?'); params_m.append(ref_id)
    if tipo and tipo != 'entrada': cond_m.append('m.tipo=?'); params_m.append(tipo)
    elif tipo == 'entrada': cond_m.append('1=0')
    if operador: cond_m.append("m.operador=?"); params_m.append(operador)
    if fecha_desde: cond_m.append("m.fecha >= ?"); params_m.append(fecha_desde + ' 00:00:00')
    if fecha_hasta: cond_m.append("m.fecha <= ?"); params_m.append(fecha_hasta + ' 23:59:59')

    movs = conn.execute(f'''
        SELECT m.fecha, m.tipo, r.nombre as referencia, m.racks as ubicacion,
               COALESCE(m.unidades,0) as unidades, m.lote, m.notas,
               COALESCE(m.operador,'') as operador
        FROM movimientos m LEFT JOIN referencias r ON m.referencia_id=r.id
        WHERE {" AND ".join(cond_m)} ORDER BY m.fecha DESC
    ''', params_m).fetchall()

    cond_i, params_i = ['1=1'], []
    if ref_id:   cond_i.append('i.referencia_id=?'); params_i.append(ref_id)
    if operador: cond_i.append("i.operador=?"); params_i.append(operador)
    if fecha_desde: cond_i.append("i.fecha_entrada >= ?"); params_i.append(fecha_desde + ' 00:00:00')
    if fecha_hasta: cond_i.append("i.fecha_entrada <= ?"); params_i.append(fecha_hasta + ' 23:59:59')

    entradas = []
    if not tipo or tipo == 'entrada':
        rows_i = conn.execute(f'''
            SELECT i.fecha_entrada as fecha, 'entrada' as tipo,
                   r.nombre as referencia,
                   (p.rack||'-'||p.nivel||'-'||p.posicion) as ubicacion,
                   COALESCE(i.unidades, r.unidades_por_estiba) as unidades,
                   i.lote, '' as notas,
                   COALESCE(i.operador,'') as operador
            FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
            JOIN referencias r ON i.referencia_id=r.id
            WHERE {" AND ".join(cond_i)} ORDER BY i.fecha_entrada DESC
        ''', params_i).fetchall()
        entradas = [dict(r) for r in rows_i]

    conn.close()
    todos = [dict(m) for m in movs] + entradas
    todos.sort(key=lambda x: x['fecha'], reverse=True)

    df = pd.DataFrame(todos, columns=['fecha','tipo','referencia','ubicacion','unidades','lote','notas','operador'])
    df.columns = ['Fecha','Tipo','Referencia','Ubicación','Unidades','Lote','Notas','Operador']
    out = io.BytesIO()
    with pd.ExcelWriter(out, engine='openpyxl') as w:
        df.to_excel(w, index=False, sheet_name='Kárdex')
    out.seek(0)
    fname = f'kardex_{datetime.now().strftime("%Y%m%d_%H%M")}.xlsx'
    return send_file(out, mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                     as_attachment=True, download_name=fname)

@app.route('/api/kardex')
def api_kardex():
    conn = get_db()
    ref_id   = request.args.get('ref_id', '')
    tipo     = request.args.get('tipo', '')      # entrada|salida|traslado|salida_parcial
    operador = request.args.get('operador', '')
    fecha_desde = request.args.get('desde', '')
    fecha_hasta = request.args.get('hasta', '')
    page     = int(request.args.get('page', 1))
    per_page = 50

    # ── Movimientos (salidas, traslados) ──
    cond_m, params_m = ['1=1'], []
    if ref_id:
        cond_m.append('m.referencia_id=?'); params_m.append(ref_id)
    if tipo and tipo != 'entrada':
        cond_m.append('m.tipo=?'); params_m.append(tipo)
    elif tipo == 'entrada':
        cond_m.append('1=0')   # entradas vienen de inventario
    if operador:
        cond_m.append("m.operador=?"); params_m.append(operador)
    if fecha_desde:
        cond_m.append("m.fecha >= ?"); params_m.append(fecha_desde + ' 00:00:00')
    if fecha_hasta:
        cond_m.append("m.fecha <= ?"); params_m.append(fecha_hasta + ' 23:59:59')

    movs = conn.execute(f'''
        SELECT m.id, m.tipo, m.racks as ubicacion, m.fecha, m.lote, m.notas,
               COALESCE(m.unidades,0) as unidades, COALESCE(m.estibas,0) as estibas,
               r.nombre as referencia, r.id as ref_id, r.color,
               COALESCE(m.operador,'') as operador
        FROM movimientos m LEFT JOIN referencias r ON m.referencia_id=r.id
        WHERE {" AND ".join(cond_m)}
        ORDER BY m.fecha DESC
    ''', params_m).fetchall()

    # ── Entradas (desde inventario) ──
    cond_i, params_i = ['1=1'], []
    if ref_id:
        cond_i.append('i.referencia_id=?'); params_i.append(ref_id)
    if operador:
        cond_i.append("i.operador=?"); params_i.append(operador)
    if fecha_desde:
        cond_i.append("i.fecha_entrada >= ?"); params_i.append(fecha_desde + ' 00:00:00')
    if fecha_hasta:
        cond_i.append("i.fecha_entrada <= ?"); params_i.append(fecha_hasta + ' 23:59:59')

    entradas = []
    if not tipo or tipo == 'entrada':
        rows_i = conn.execute(f'''
            SELECT i.id, 'entrada' as tipo,
                   (p.rack||'-'||p.nivel||'-'||p.posicion) as ubicacion,
                   i.fecha_entrada as fecha, i.lote,
                   '' as notas,
                   COALESCE(i.unidades, r.unidades_por_estiba) as unidades,
                   1 as estibas,
                   r.nombre as referencia, r.id as ref_id, r.color,
                   COALESCE(i.operador,'') as operador
            FROM inventario i
            JOIN posiciones p ON i.posicion_id=p.id
            JOIN referencias r ON i.referencia_id=r.id
            WHERE {" AND ".join(cond_i)}
            ORDER BY i.fecha_entrada DESC
        ''', params_i).fetchall()
        entradas = [dict(r) for r in rows_i]

    # Combinar y ordenar
    todos = [dict(m) for m in movs] + entradas
    todos.sort(key=lambda x: x['fecha'], reverse=True)

    # Stats
    total_entradas = sum(r['unidades'] for r in todos if r['tipo'] == 'entrada')
    total_salidas  = sum(r['unidades'] for r in todos if r['tipo'] in ('salida','salida_parcial'))
    total_traslados = sum(1 for r in todos if r['tipo'] == 'traslado')

    # Paginación
    total = len(todos)
    inicio = (page - 1) * per_page
    pagina = todos[inicio:inicio + per_page]

    # Operadores disponibles
    ops = conn.execute("SELECT DISTINCT operador FROM movimientos WHERE operador != '' UNION SELECT DISTINCT operador FROM inventario WHERE operador != '' ORDER BY operador").fetchall()

    conn.close()
    return jsonify({
        'movimientos': pagina,
        'total': total,
        'page': page,
        'pages': max(1, -(-total // per_page)),
        'stats': {
            'entradas': total_entradas,
            'salidas': total_salidas,
            'traslados': total_traslados,
            'total_movs': total
        },
        'operadores': [o[0] for o in ops]
    })

@app.route('/api/movimientos_recientes')
def movimientos_recientes():
    conn = get_db()
    try: conn.execute("ALTER TABLE movimientos ADD COLUMN foto TEXT DEFAULT ''")
    except: pass
    conn.commit()
    movs = conn.execute('''
        SELECT m.id, m.tipo, m.racks, m.fecha, m.lote, m.notas, m.unidades,
               r.nombre as referencia, r.color, m.referencia_id,
               COALESCE(m.operador,'') as operador,
               COALESCE(m.foto,'') as foto
        FROM movimientos m LEFT JOIN referencias r ON m.referencia_id = r.id
        WHERE m.tipo IN ('traslado','salida','salida_parcial')
        ORDER BY m.fecha DESC LIMIT 40
    ''').fetchall()
    traslados = []
    for m in movs:
        row = dict(m)
        if m['tipo'] == 'traslado' and m['racks'] and ' → ' in m['racks']:
            destino = m['racks'].split(' → ')[1].strip()
            partes = destino.split('-')
            if len(partes) == 3:
                inv = conn.execute('''SELECT i.id as inv_id, i.actualizado_erp
                    FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
                    WHERE p.rack=? AND p.nivel=? AND p.posicion=?''',
                    (partes[0], partes[1], partes[2])).fetchone()
                if inv:
                    row['inv_id'] = inv['inv_id']
                    row['actualizado_erp'] = inv['actualizado_erp']
        traslados.append(row)
    entradas = conn.execute('''
        SELECT i.id, 'entrada' as tipo,
               (p.rack||'-'||p.nivel||'-'||p.posicion) as racks,
               i.fecha_entrada as fecha, i.lote, '' as notas, i.unidades as unidades,
               r.nombre as referencia, r.color,
               i.actualizado_erp, i.id as inv_id,
               p.rack, p.nivel, p.posicion, r.unidades_por_estiba,
               COALESCE(i.operador,'') as operador
        FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
        JOIN referencias r ON i.referencia_id=r.id
        ORDER BY i.fecha_entrada DESC LIMIT 30
    ''').fetchall()
    conn.close()
    combined = traslados + [dict(r) for r in entradas]
    combined.sort(key=lambda x: x['fecha'], reverse=True)
    return jsonify(combined[:60])

@app.route('/api/stock_referencias')
def stock_referencias():
    conn = get_db()
    refs = conn.execute('''
        SELECT r.id, r.nombre, r.grupo, r.marca, r.color,
               r.unidades_por_estiba, r.peso_unidad,
               COALESCE(SUM(CASE WHEN i.unidades > 0 THEN i.unidades ELSE r.unidades_por_estiba END), 0) as total_unidades,
               COUNT(i.id) as total_estibas,
               COUNT(CASE WHEN i.actualizado_erp=0 THEN 1 END) as pendientes_erp,
               MAX(i.fecha_entrada) as ultima_entrada
        FROM referencias r
        JOIN inventario i ON i.referencia_id = r.id
        GROUP BY r.id
        ORDER BY r.grupo, r.nombre
    ''').fetchall()

    ubics = conn.execute('''
        SELECT i.referencia_id, i.unidades, i.lote, i.fecha_entrada,
               i.actualizado_erp, r.unidades_por_estiba,
               p.rack||'-'||p.nivel||'-'||p.posicion as ubicacion,
               p.rack
        FROM inventario i
        JOIN referencias r ON i.referencia_id = r.id
        JOIN posiciones p ON i.posicion_id = p.id
        ORDER BY p.rack, p.nivel, p.posicion
    ''').fetchall()

    ubics_por_ref = {}
    for u in ubics:
        rid = u['referencia_id']
        if rid not in ubics_por_ref:
            ubics_por_ref[rid] = []
        unis = u['unidades'] if u['unidades'] and u['unidades'] > 0 else u['unidades_por_estiba']
        ubics_por_ref[rid].append({
            'ubicacion': u['ubicacion'], 'rack': u['rack'],
            'unidades': unis, 'lote': u['lote'] or '',
            'fecha_entrada': (u['fecha_entrada'] or '')[:10],
            'erp': bool(u['actualizado_erp'])
        })

    result = []
    for r in refs:
        result.append({
            'id': r['id'], 'nombre': r['nombre'],
            'grupo': r['grupo'] or '', 'marca': r['marca'] or '',
            'color': r['color'] or '#888',
            'total_unidades': int(r['total_unidades']),
            'total_estibas': r['total_estibas'],
            'pendientes_erp': r['pendientes_erp'],
            'ultima_entrada': (r['ultima_entrada'] or '')[:10],
            'ubicaciones': ubics_por_ref.get(r['id'], [])
        })
    conn.close()
    return jsonify(result)

@app.route('/api/resumen_inventario')
def resumen_inventario():
    conn = get_db()
    rows = conn.execute('''
        SELECT r.nombre as sku, r.grupo, r.marca, r.color,
               r.unidades_por_estiba, r.peso_unidad,
               COALESCE(SUM(CASE WHEN i.unidades > 0 THEN i.unidades ELSE r.unidades_por_estiba END), 0) as total_unidades,
               COUNT(i.id) as total_estibas,
               COUNT(CASE WHEN i.actualizado_erp=0 THEN 1 END) as pendientes_erp
        FROM referencias r
        LEFT JOIN inventario i ON i.referencia_id = r.id
        GROUP BY r.id
        HAVING total_estibas > 0
        ORDER BY r.grupo, r.nombre
    ''').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    app.logger.error(tb)
    return jsonify({'error': str(e), 'tipo': type(e).__name__, 'detalle': tb}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)