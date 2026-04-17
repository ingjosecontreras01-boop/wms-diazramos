from flask import Flask, render_template, request, jsonify, send_file, send_from_directory
import sqlite3, pandas as pd, io, os, traceback
from datetime import datetime

app = Flask(__name__, template_folder='.', static_folder='.', static_url_path='/static')
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
        peso_unidad REAL DEFAULT 0,
        unidades_por_estiba INTEGER NOT NULL DEFAULT 1,
        color TEXT DEFAULT '#0782c2'
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS posiciones (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        rack TEXT NOT NULL, nivel TEXT NOT NULL, posicion TEXT NOT NULL,
        activa INTEGER DEFAULT 1, UNIQUE(rack, nivel, posicion)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS inventario (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        posicion_id INTEGER NOT NULL UNIQUE, referencia_id INTEGER NOT NULL,
        lote TEXT DEFAULT '', fecha_entrada TEXT NOT NULL,
        actualizado_erp INTEGER DEFAULT 0,
        FOREIGN KEY (posicion_id) REFERENCES posiciones(id),
        FOREIGN KEY (referencia_id) REFERENCES referencias(id)
    )''')
    c.execute('''CREATE TABLE IF NOT EXISTS movimientos (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        tipo TEXT NOT NULL, referencia_id INTEGER, lote TEXT,
        unidades INTEGER DEFAULT 0, estibas INTEGER DEFAULT 0,
        racks TEXT DEFAULT '', fecha TEXT NOT NULL, notas TEXT DEFAULT ''
    )''')
    for rack, niveles in ESTRUCTURA_RACKS.items():
        for nivel, posiciones in niveles.items():
            for pos in posiciones:
                c.execute('INSERT OR IGNORE INTO posiciones (rack,nivel,posicion,activa) VALUES (?,?,?,1)',
                          (rack, nivel, pos))
    conn.commit()
    conn.close()

init_db()

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
    refs = conn.execute('SELECT * FROM referencias ORDER BY nombre').fetchall()
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
        existing = conn.execute('SELECT COUNT(*) FROM referencias').fetchone()[0]
        for i, row in df.iterrows():
            nombre = str(row.iloc[0]).strip()
            if not nombre or nombre == 'nan': continue
            peso = float(row.iloc[1]) if len(row) > 1 and str(row.iloc[1]) != 'nan' else 0
            upE = int(row.iloc[2]) if len(row) > 2 and str(row.iloc[2]) != 'nan' else 1
            color = COLORES[(existing + count) % len(COLORES)]
            try:
                conn.execute('INSERT OR IGNORE INTO referencias (nombre,peso_unidad,unidades_por_estiba,color) VALUES (?,?,?,?)',
                             (nombre, peso, upE, color))
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
    if not posiciones_estibas:
        return jsonify({'error': 'Sin posiciones'}), 400
    conn = get_db()
    fecha = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    registros = []
    for pe in posiciones_estibas:
        pos_id = pe.get('posicion_id')
        lotes = pe.get('lotes', [])
        lote_str = ' | '.join([f"{l['lote']}({l.get('cantidad',0)}u)" for l in lotes if l.get('lote')]) if lotes else ''
        registros.append((pos_id, ref_id, lote_str, fecha))
    conn.executemany('INSERT OR IGNORE INTO inventario (posicion_id,referencia_id,lote,fecha_entrada) VALUES (?,?,?,?)', registros)
    pos_ocupadas = []
    for r in registros:
        ya = conn.execute('SELECT id FROM inventario WHERE posicion_id=? AND referencia_id!=?', (r[0], ref_id)).fetchone()
        if ya: pos_ocupadas.append(r[0])
    conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha) VALUES (?,?,?,?,?,?,?)',
                 ('entrada', ref_id, '', unidades, estibas, rack, fecha))
    conn.commit()
    conn.close()
    resp = {'ok': True, 'registradas': len(registros)}
    if pos_ocupadas:
        resp['advertencia'] = f'{len(pos_ocupadas)} posición(es) ya estaban ocupadas y fueron omitidas'
    return jsonify(resp)

@app.route('/api/estado_racks')
def estado_racks():
    conn = get_db()
    rows = conn.execute('''
        SELECT p.id, p.rack, p.nivel, p.posicion, p.activa,
               i.id as inv_id, i.lote, i.fecha_entrada, i.actualizado_erp,
               r.nombre as referencia, r.id as ref_id, r.color, r.unidades_por_estiba
        FROM posiciones p
        LEFT JOIN inventario i ON p.id = i.posicion_id
        LEFT JOIN referencias r ON i.referencia_id = r.id
        ORDER BY p.rack, p.nivel, p.posicion
    ''').fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

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
    rows = conn.execute('''
        SELECT i.id, i.lote, i.fecha_entrada, i.actualizado_erp,
               p.rack, p.nivel, p.posicion, r.nombre as referencia, r.color
        FROM inventario i JOIN posiciones p ON i.posicion_id = p.id
        JOIN referencias r ON i.referencia_id = r.id
        WHERE i.lote LIKE ? ORDER BY i.fecha_entrada DESC
    ''', (f'%{lote}%',)).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])

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
    data = request.json
    inv_id = data.get('inv_id')
    tipo = data.get('tipo', 'total')
    unidades = data.get('unidades')
    nota = data.get('nota', '')
    if not inv_id:
        return jsonify({'error': 'Faltan parámetros'}), 400
    conn = get_db()
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
        conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,notas) VALUES (?,?,?,?,?,?,?,?)',
                     ('salida', inv['ref_id'], inv['lote'], 0, 1, codigo, fecha, nota or 'Salida total'))
    else:
        conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,notas) VALUES (?,?,?,?,?,?,?,?)',
                     ('salida_parcial', inv['ref_id'], inv['lote'], unidades or 0, 0, codigo, fecha,
                      nota or f'Salida parcial: {unidades} u'))
    conn.commit()
    conn.close()
    return jsonify({'ok': True})

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
    origen = f"{inv['rack']}-{inv['nivel']}-{inv['posicion']}"
    destino = f"{nueva_pos['rack']}-{nueva_pos['nivel']}-{nueva_pos['posicion']}"
    conn.execute('UPDATE inventario SET posicion_id=?, actualizado_erp=0 WHERE id=?', (nueva_posicion_id, inv_id))
    conn.execute('INSERT INTO movimientos (tipo,referencia_id,lote,unidades,estibas,racks,fecha,notas) VALUES (?,?,?,?,?,?,?,?)',
                 ('traslado', inv['referencia_id'], inv['lote'], 0, 1, f"{origen} → {destino}", fecha, 'Traslado'))
    conn.commit()
    conn.close()
    return jsonify({'ok': True, 'nueva_ubicacion': destino, 'origen': origen})

@app.route('/api/movimientos_recientes')
def movimientos_recientes():
    conn = get_db()
    movs = conn.execute('''
        SELECT m.id, m.tipo, m.racks, m.fecha, m.lote, m.notas, m.unidades,
               r.nombre as referencia, r.color, m.referencia_id
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
               i.fecha_entrada as fecha, i.lote, '' as notas, 0 as unidades,
               r.nombre as referencia, r.color,
               i.actualizado_erp, i.id as inv_id,
               p.rack, p.nivel, p.posicion, r.unidades_por_estiba
        FROM inventario i JOIN posiciones p ON i.posicion_id=p.id
        JOIN referencias r ON i.referencia_id=r.id
        ORDER BY i.fecha_entrada DESC LIMIT 30
    ''').fetchall()
    conn.close()
    combined = traslados + [dict(r) for r in entradas]
    combined.sort(key=lambda x: x['fecha'], reverse=True)
    return jsonify(combined[:60])

@app.errorhandler(Exception)
def handle_exception(e):
    tb = traceback.format_exc()
    app.logger.error(tb)
    return jsonify({'error': str(e), 'tipo': type(e).__name__, 'detalle': tb}), 500

if __name__ == '__main__':
    port = int(os.environ.get('PORT', 5000))
    app.run(debug=True, host='0.0.0.0', port=port)