from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse
import pandas as pd
from backend.db import get_connection
from backend.predictor import predecir_riesgo_por_rut
from backend.predictor_hibrido import predecir_riesgo_hibrido
from backend.evaluar_riesgo_heuristico import obtener_riesgo_por_rut
from backend.riesgo_academico import calcular_riesgo_academico
from backend.riesgo_psicologico import calcular_riesgo_psicologico
from backend.riesgo_interseccional import calcular_riesgo_interseccional
from backend.riesgo_global import combinar_niveles
from weasyprint import HTML
import tempfile
from datetime import datetime
import os



app = FastAPI()

# CORS para frontend Vue
app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:8080"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 📌 Obtener todos los estudiantes
@app.get("/estudiantes")
def leer_estudiantes():
    conn = get_connection()
    query = """
        SELECT [RUT], [NOMBRE COMPLETO], [Carrera], [AÑO DE INGRESO],
               [Ciudad], [Via de Ingreso], [Estado]
        FROM dbo.PACE2024_ACTUALIZADO
    """
    df = pd.read_sql(query, conn)
    return df.to_dict(orient="records")

# 📌 Riesgo basado en predictor simple
@app.get("/riesgo/{rut}")
def obtener_riesgo(rut: str):
    resultado = predecir_riesgo_por_rut(rut)
    if resultado is None:
        raise HTTPException(status_code=404, detail="Estudiante no encontrado")
    return resultado

# 📌 Riesgo heurístico completo
@app.get("/riesgo_heuristico/{rut}")
def riesgo_heuristico(rut: str):
    resultado = obtener_riesgo_por_rut(rut)
    if resultado:
        return resultado
    return {"error": "RUT no encontrado"}

# 📌 Riesgo híbrido + historial
@app.get("/riesgo_hibrido/{rut}")
def riesgo_hibrido(rut: str):
    resultado = predecir_riesgo_hibrido(rut)
    if resultado is None:
        return {"mensaje": "Estudiante no encontrado"}

    conn = get_connection()
    query = """
        SELECT 
            FORMAT(FechaEvaluacion, 'dd ''de'' MMMM ''de'' yyyy', 'es-ES') AS fecha,
            NivelRiesgo AS nivel
        FROM dbo.EvaluacionRiesgo
        WHERE Run = ?
        ORDER BY FechaEvaluacion DESC
    """
    df_hist = pd.read_sql(query, conn, params=[rut])
    resultado["historial"] = df_hist.to_dict(orient="records")
    return resultado

# 📌 Riesgos ya calculados desde la base
@app.get("/riesgos_calculados")
def riesgos_calculados():
    conn = get_connection()
    query = """
        SELECT
            er.Run AS rut,
            er.NombreCompleto AS nombre,
            er.Carrera AS carrera,
            er.NivelRiesgo AS riesgo,
            p.[AÑO DE INGRESO] AS anio_ingreso
        FROM dbo.EvaluacionRiesgo er
        LEFT JOIN [PBI_Docencia].[dbo].[PACE2024_ACTUALIZADO] p
            ON er.Run = p.RUT
    """
    df = pd.read_sql(query, conn)
    return df.to_dict(orient="records")

# ✅ Nuevo: Riesgo académico por RUT
@app.get("/riesgo/academico/{rut}")
def riesgo_academico(rut: str):
    conn = get_connection()
    query = """
        WITH NotasCriticas AS (
            SELECT 
                RUT,
                COUNT(DISTINCT [Denominación Actividad Curricular]) AS RamosReprobados
            FROM [dbo].[NotasPace2025]
            CROSS APPLY (
                SELECT TRY_CAST(REPLACE([Nota_1], ',', '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_2], ',', '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_3], ',', '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_4], ',', '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_5], ',', '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_6], ',', '.') AS FLOAT)
            ) AS Notas(nota)
            WHERE nota < 4.0
            GROUP BY RUT
        )
        SELECT RamosReprobados
        FROM NotasCriticas
        WHERE RUT = ?
    """
    df = pd.read_sql(query, conn, params=[rut])
    ramos = int(df["RamosReprobados"].iloc[0]) if not df.empty else 0

    puntaje, nivel, motivo = calcular_riesgo_academico(ramos)
    return {"puntaje": puntaje, "riesgo": nivel, "motivo": motivo}

# ✅ Nuevo: Riesgo psicológico por RUT
@app.get("/riesgo/psicologico/{rut}")
def riesgo_psicologico(rut: str):
    conn = get_connection()
    query = """
        SELECT 
            [PROMEDIO AUTOEFICACIA ACADÉMICA],
            [PROMEDIO MODULACIÓN EMOCIONAL],
            [PROMEDIO AUTODETERMINACIÓN PERSONAL],
            [PROMEDIO SOCIABILIDAD],
            [PROMEDIO PROSPECTIVA ACADÉMICA]
        FROM dbo.[Epaes$]
        WHERE RUT = ?
    """
    df = pd.read_sql(query, conn, params=[rut])

    if df.empty:
        raise HTTPException(status_code=404, detail="Estudiante no tiene datos psicológicos")

    row = df.iloc[0]
    p, nivel, motivos = calcular_riesgo_psicologico(
        row[0], row[1], row[2], row[3], row[4]
    )
    return {"puntaje": p, "riesgo": nivel, "motivos": motivos}

# ✅ Nuevo: Riesgo interseccional (se recibe desde frontend)
@app.post("/riesgo/interseccional")
def riesgo_interseccional(vulnerabilidades: list):
    puntaje, nivel, detalle = calcular_riesgo_interseccional(vulnerabilidades)
    return {"puntaje": puntaje, "riesgo": nivel, "detalle": detalle}

@app.get("/riesgo/global/{rut}")
def riesgo_global(rut: str):
    conn = get_connection()

    # Obtener riesgo académico
    query_ramos = """
        WITH NotasCriticas AS (
            SELECT 
                RUT,
                COUNT(DISTINCT [Denominación Actividad Curricular]) AS RamosReprobados
            FROM [dbo].[NotasPace2025]
            CROSS APPLY (
                SELECT TRY_CAST(REPLACE([Nota_1], ',' , '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_2], ',' , '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_3], ',' , '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_4], ',' , '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_5], ',' , '.') AS FLOAT) UNION ALL
                SELECT TRY_CAST(REPLACE([Nota_6], ',' , '.') AS FLOAT)
            ) AS Notas(nota)
            WHERE nota < 4.0
            GROUP BY RUT
        )
        SELECT RamosReprobados
        FROM NotasCriticas
        WHERE RUT = ?
    """
    df_ramos = pd.read_sql(query_ramos, conn, params=[rut])
    ramos = int(df_ramos["RamosReprobados"].iloc[0]) if not df_ramos.empty else 0
    puntaje_a, nivel_a, motivo_a = calcular_riesgo_academico(ramos)

    # Obtener riesgo psicológico
    query_psico = """
       SELECT
            [PROMEDIO AUTOEFICACIA ACADÉMICA],
            [PROMEDIO MODULACIÓN EMOCIONAL],
            [PROMEDIO AUTODETERMINACIÓN PERSONAL],
            [PROMEDIO SOCIABILIDAD],
            [PROMEDIO PROSPECTIVA ACADÉMICA]
        FROM dbo.[Epaes$]
        WHERE RUT = ?
    """
    df_psico = pd.read_sql(query_psico, conn, params=[rut])
    if df_psico.empty:
        nivel_p = "Bajo"
        puntaje_p = 0
        motivos_p = "Sin datos"
    else:
        row = df_psico.iloc[0]
        puntaje_p, nivel_p, motivos_p = calcular_riesgo_psicologico(
            row.iloc[0], row.iloc[1], row.iloc[2], row.iloc[3], row.iloc[4]
        )

    # Riesgo interseccional dinámico
    query_inter = """
        SELECT
            [Género],
            [¿Eres padre/madre?],
            [Durante el año, ¿trabajarás para costear tus estudios y gastos p],
            [(5) ¿Cuentas con alguna beca?],
            [¿Cuentas con algún crédito universitario?],
            [¿Hay algún otro miembro de tu núcleo familiar que haya ingresado],
            [(3) ¿Tienes algún tipo de discapacidad?],
            [(4) ¿Presentas alguna condición de salud que ha dificultado tus ],
            [Fecha de nacimiento]
        FROM [PBI_Docencia].[dbo].[Caracterizacion_Ingreso$]
        WHERE [Institución] = ?
    """
    df_inter = pd.read_sql(query_inter, conn, params=[rut])

    if df_inter.empty:
        nivel_i = "Bajo"
        puntaje_i = 0
        detalle_i = "Sin datos"
    else:
        row = df_inter.iloc[0]
        vulnerabilidades = []
        vulnerabilidades.append(1 if row['Género'] in ['Masculino', 'Femenino', 'Prefiero no especificar'] else 0)
        vulnerabilidades.append(1 if row['¿Hay algún otro miembro de tu núcleo familiar que haya ingresado'] == 'No' else 0)
        vulnerabilidades.append(1 if row['(5) ¿Cuentas con alguna beca?'] == 'Sí' else 0)
        vulnerabilidades.append(1 if row['(4) ¿Presentas alguna condición de salud que ha dificultado tus '] == 'Sí' else 0)
        vulnerabilidades.append(1 if row['(3) ¿Tienes algún tipo de discapacidad?'] == 'Sí' else 0)

        # Corregir parseo de fecha en español
        fecha = pd.to_datetime(row['Fecha de nacimiento'], format='%d %B %Y', dayfirst=True, errors='coerce')
        if pd.notna(fecha):
            edad = pd.Timestamp.now().year - fecha.year
        else:
            edad = 0
        vulnerabilidades.append(1 if edad >= 25 else 0)

        vulnerabilidades.append(1 if row['Durante el año, ¿trabajarás para costear tus estudios y gastos p'] == 'Sí' else 0)
        vulnerabilidades.append(1 if row['¿Eres padre/madre?'] == 'Sí' else 0)

        #nombres legibles para el detalle
        factores_nombre = [
            "Identidad de género",
            "Primera generación",
            "Nivel socioeconómico",
            "Salud mental",
            "Discapacidad",
            "Edad >= 25",
            "Trabajador",
            "Padre/madre"
        ]
        factores_activados = [factores_nombre[i] for i, valor in enumerate(vulnerabilidades) if valor == 1]
        detalle_i = f"Factores activados: {', '.join(factores_activados)}"

        puntaje_i, nivel_i, _ = calcular_riesgo_interseccional(vulnerabilidades)

    # Riesgo global final
    riesgo_global = combinar_niveles([nivel_a, nivel_p, nivel_i])

    # Obtener nombre completo y carrera desde la tabla PACE2024_ACTUALIZADO
    query_datos_basicos = """
        SELECT
            [NOMBRE COMPLETO] AS NombreCompleto,
            [Carrera]
        FROM [PBI_Docencia].[dbo].[PACE2024_ACTUALIZADO]
        WHERE [RUT] = ?
    """
    df_basico = pd.read_sql(query_datos_basicos, conn, params=[rut])

    if df_basico.empty:
        nombre_completo = "No encontrado"
        carrera = "No encontrada"
    else:
        nombre_completo = df_basico.iloc[0]['NombreCompleto']
        carrera = df_basico.iloc[0]['Carrera']

    query_recomendaciones = """
        SELECT Acciones
        FROM Recomendaciones
        WHERE TipoRiesgo = ? AND NivelRiesgo = ?
    """
    
    #Recomendaciones para el riesgo psicologico
    df_recomendacion_psico = pd.read_sql(query_recomendaciones, conn, params=['Psicológico', nivel_p])
    recomendacion_psico = df_recomendacion_psico.iloc[0]['Acciones'] if not df_recomendacion_psico.empty else "Sin recomendaciones"

    #seccion riesgos academicos
    recomendacion_academica = "Sin recomendacion"
    # Insertar evaluación en la BD (sin actualizar)
    cursor = conn.cursor()
    query_insert = """
    INSERT INTO [dbo].[EvaluacionDeRut]
        (Run, NombreCompleto, Carrera, NivelRiesgo, NivelRiesgoAcademico, NivelRiesgoPsicologico, NivelRiesgoInterseccional, FechaEvaluacion)
    VALUES (?, ?, ?, ?, ?, ?, ?, GETDATE());
    """
    cursor.execute(query_insert, (
        rut, nombre_completo, carrera, riesgo_global, nivel_a, nivel_p, nivel_i
    ))
    conn.commit()

    #trae info al modal psicologico
    query_factores_psico = """
        SELECT esta_recibiendo_apoyo, nombre_profesional, observaciones
        FROM FactoresPsicologicos
        WHERE rut_estudiante = ?
    """
    df_factores_psico = pd.read_sql(query_factores_psico, conn, params=[rut])

    #por defecto, campos vacios
    factores_psico_data = {
        "esta_recibiendo_apoyo": 0,
        "nombre_profesional": "",
        "observaciones": ""
    }

    #si existe registro, actualiza el objeto
    if not df_factores_psico.empty:
        row_factores = df_factores_psico.iloc[0]
        factores_psico_data = {
            "esta_recibiendo_apoyo": int(row_factores["esta_recibiendo_apoyo"]),
            "nombre_profesional": row_factores["nombre_profesional"] or "",
            "observaciones": row_factores["observaciones"] or ""
        }


    # ✅ Retornar resultados
    return {
        "rut": rut,
        "nombre_completo": nombre_completo,
        "carrera": carrera,
        "riesgo_global": riesgo_global,
        "riesgos": {
            "academico": {
                 "nivel": nivel_a, 
                 "puntaje": puntaje_a, 
                 "motivo": motivo_a,
                 "recomendacion": recomendacion_academica
                 },
            "psicologico": {
                "nivel": nivel_p, 
                "puntaje": puntaje_p, 
                "motivos": motivos_p,
                "recomendacion": recomendacion_psico
                },
            "interseccional": {
                "nivel": nivel_i, 
                "puntaje": puntaje_i, 
                "detalle": detalle_i
            }
        },
        "factores_psicologicos": factores_psico_data
    }



@app.get("/notas/{rut}")
def obtener_notas(rut:str):
    conn = get_connection()
    query = """
            SELECT [Denominación Actividad Curricular], [Nota_1], [Nota_2], [Nota_3],
            [Nota_4], [Nota_5], [Nota_6]
            FROM [dbo].[NotasPace2025]
            WHERE RUT = ?
        """
    df = pd.read_sql(query, conn, params=[rut])
    return df.to_dict(orient="records")
    
@app.get("/reporte/{rut}")
def generar_reporte_pdf(rut: str):
    conn = get_connection()

    # 1️⃣ Datos generales del estudiante
    query_estudiante = """
        SELECT [RUT], [NOMBRE COMPLETO], [Carrera], [AÑO DE INGRESO],
               [Ciudad], [Via de Ingreso], [Estado]
        FROM dbo.PACE2024_ACTUALIZADO
        WHERE RUT = ?
    """
    df_est = pd.read_sql(query_estudiante, conn, params=[rut])
    if df_est.empty:
        return {"error": "Estudiante no encontrado"}
    estudiante = df_est.iloc[0]

    # 2️⃣ Datos de riesgo global e individuales
    query_riesgo = """
        SELECT NivelRiesgo, NivelRiesgoAcademico, NivelRiesgoPsicologico, NivelRiesgoInterseccional
        FROM dbo.EvaluacionDeRut
        WHERE Run = ?
        ORDER BY FechaEvaluacion DESC
    """
    df_riesgo = pd.read_sql(query_riesgo, conn, params=[rut])
    if df_riesgo.empty:
        nivel_global = "No evaluado"
        nivel_academico = "No evaluado"
        nivel_psicologico = "No evaluado"
        nivel_interseccional = "No evaluado"
    else:
        riesgo = df_riesgo.iloc[0]
        nivel_global = riesgo["NivelRiesgo"]
        nivel_academico = riesgo["NivelRiesgoAcademico"]
        nivel_psicologico = riesgo["NivelRiesgoPsicologico"]
        nivel_interseccional = riesgo["NivelRiesgoInterseccional"]

    # 3️⃣ Notas del estudiante
    query_notas = """
        SELECT [Denominación Actividad Curricular], [Nota_1], [Nota_2], [Nota_3], [Nota_4], [Nota_5], [Nota_6]
        FROM dbo.NotasPace2025
        WHERE RUT = ?
    """
    df_notas = pd.read_sql(query_notas, conn, params=[rut])

    # 4️⃣ Preparar el HTML con nuevo diseño
    html_content = f"""
    <html>
    <head>
    <style>
        body {{
            font-family: Arial, sans-serif;
            color: #333;
            margin: 40px;
        }}
        h1 {{
            color: #1a73e8;
            border-bottom: 2px solid #1a73e8;
            padding-bottom: 5px;
        }}
        .section {{
            margin-top: 20px;
        }}
        .section-title {{
            color: #1a73e8;
            font-size: 18px;
            margin-bottom: 10px;
        }}
        table {{
            width: 100%;
            border-collapse: collapse;
            margin-top: 10px;
        }}
        th, td {{
            border: 1px solid #ccc;
            padding: 8px;
            text-align: left;
        }}
        th {{
            background-color: #f2f2f2;
        }}
        .highlight {{
            font-weight: bold;
        }}
    </style>
    </head>
    <body>
        <h1>Reporte del Estudiante</h1>

        <div class="section">
            <div class="section-title">Datos Generales</div>
            <p><span class="highlight">RUT:</span> {estudiante['RUT']}</p>
            <p><span class="highlight">Nombre:</span> {estudiante['NOMBRE COMPLETO']}</p>
            <p><span class="highlight">Carrera:</span> {estudiante['Carrera']}</p>
            <p><span class="highlight">Año de Ingreso:</span> {estudiante['AÑO DE INGRESO']}</p>
            <p><span class="highlight">Estado:</span> {estudiante['Estado']}</p>
        </div>

        <div class="section">
            <div class="section-title">Niveles de Riesgo</div>
            <table>
                <tr><th>Global</th><th>Académico</th><th>Psicológico</th><th>Interseccional</th></tr>
                <tr>
                    <td>{nivel_global}</td>
                    <td>{nivel_academico}</td>
                    <td>{nivel_psicologico}</td>
                    <td>{nivel_interseccional}</td>
                </tr>
            </table>
        </div>

        <div class="section">
            <div class="section-title">Notas del Estudiante</div>
            <table>
                <tr>
                    <th>Ramo</th>
                    <th>Nota 1</th>
                    <th>Nota 2</th>
                    <th>Nota 3</th>
                    <th>Nota 4</th>
                    <th>Nota 5</th>
                    <th>Nota 6</th>
                </tr>
    """

    for idx, row in df_notas.iterrows():
        html_content += f"""
            <tr>
                <td>{row['Denominación Actividad Curricular']}</td>
                <td>{row['Nota_1']}</td>
                <td>{row['Nota_2']}</td>
                <td>{row['Nota_3']}</td>
                <td>{row['Nota_4']}</td>
                <td>{row['Nota_5']}</td>
                <td>{row['Nota_6']}</td>
            </tr>
        """

    html_content += """
            </table>
        </div>
    </body>
    </html>
    """

    # 5️⃣ Generar el PDF
    with tempfile.NamedTemporaryFile(delete=False, suffix=".pdf") as pdf_file:
        HTML(string=html_content).write_pdf(pdf_file.name)

    return FileResponse(pdf_file.name, filename=f"reporte_{rut}.pdf", media_type="application/pdf")

@app.post("/registrar_factores_psicologicos")
async def registrar_factores_psicologicos(request: Request):
    data = await request.json()
    rut = data.get("rut")
    nivel_riesgo = data.get("nivel_riesgo")
    esta_apoyo = data.get("esta_apoyo")
    profesional = data.get("profesional")
    observaciones = data.get("observaciones")

    conn = get_connection()
    cursor = conn.cursor()

    #verifica si ya exsite un registro
    cursor.execute("SELECT COUNT(*) FROM FactoresPsicologicos WHERE rut_estudiante = ?", rut)
    existe = cursor.fetchone()[0]

    if existe:
        #actualizar
        cursor.execute("""
            UPDATE FactoresPsicologicos
            SET nivel_riesgo = ?, esta_recibiendo_apoyo = ?, nombre_profesional = ?, observaciones = ?, fecha_registro = GETDATE()
            WHERE rut_estudiante = ?
        """, nivel_riesgo, esta_apoyo, profesional, observaciones, rut)
    else:
        #insertar
        cursor.execute("""
            INSERT INTO FactoresPsicologicos (rut_estudiante, nivel_riesgo, esta_recibiendo_apoyo, nombre_profesional, observaciones)
            VALUES (?, ?, ?, ?, ?)
        """, rut, nivel_riesgo, esta_apoyo, profesional, observaciones)

    conn.commit()
    conn.close()

    return{"mensaje": "✅ Registro actualizado correctamente"}