import pandas as pd
import pyodbc
import joblib
import os
from dotenv import load_dotenv

# Cargar variables .env
load_dotenv()

# Cargar modelo entrenado
modelo = joblib.load("modelo_hibrido.pkl")

# Conexión a SQL Server
def get_connection():
    return pyodbc.connect(
        f"DRIVER={{ODBC Driver 17 for SQL Server}};"
        f"SERVER={os.getenv('DB_SERVER')};"
        f"DATABASE={os.getenv('DB_NAME')};"
        f"UID={os.getenv('DB_USER')};"
        f"PWD={os.getenv('DB_PASSWORD')}"
    )

# Predicción por RUT
def predecir_riesgo_hibrido(rut):
    conn = get_connection()

    query = """
    WITH RamosConNotasCriticas AS (
        SELECT 
            RUT,
            COUNT(DISTINCT [Denominación Actividad Curricular]) AS RamosReprobados
        FROM [dbo].[NotasPace2025]
        CROSS APPLY (
            SELECT 
                CASE WHEN ISNUMERIC(REPLACE([Nota_1], ',', '.')) = 1 THEN CAST(REPLACE([Nota_1], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
                SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_2], ',', '.')) = 1 THEN CAST(REPLACE([Nota_2], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
                SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_3], ',', '.')) = 1 THEN CAST(REPLACE([Nota_3], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
                SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_4], ',', '.')) = 1 THEN CAST(REPLACE([Nota_4], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
                SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_5], ',', '.')) = 1 THEN CAST(REPLACE([Nota_5], ',', '.') AS FLOAT) ELSE NULL END UNION ALL
                SELECT CASE WHEN ISNUMERIC(REPLACE([Nota_6], ',', '.')) = 1 THEN CAST(REPLACE([Nota_6], ',', '.') AS FLOAT) ELSE NULL END
        ) AS Notas(nota)
        WHERE nota IS NOT NULL AND nota < 4.0
        GROUP BY RUT
    )
    SELECT 
        p.RUT,
        p.[NOMBRE COMPLETO],
        p.Carrera,
        ISNULL(r.RamosReprobados, 0) AS RamosReprobados,
        e.[PROMEDIO AUTOEFICACIA ACADÉMICA] AS Autoeficacia,
        e.[PROMEDIO MODULACIÓN EMOCIONAL] AS Emocional,
        e.[PROMEDIO AUTODETERMINACIÓN PERSONAL] AS Autodeterminacion,
        e.[PROMEDIO SOCIABILIDAD] AS Sociabilidad,
        e.[PROMEDIO PROSPECTIVA ACADÉMICA] AS Prospectiva
    FROM [dbo].[PACE2024_ACTUALIZADO] p
    LEFT JOIN RamosConNotasCriticas r ON p.RUT = r.RUT
    LEFT JOIN [dbo].[Epaes$] e ON CAST(p.RUT AS VARCHAR) = CAST(e.RUT AS VARCHAR)
    WHERE p.RUT = ?
    """

    df = pd.read_sql(query, conn, params=[rut])
    if df.empty:
        return None

    row = df.iloc[0]
    
    # Calcular puntaje heurístico
    puntaje = 0
    if row["RamosReprobados"] == 1:
        puntaje += 1
    elif row["RamosReprobados"] >= 2:
        puntaje += 2
    if pd.notna(row["Autoeficacia"]) and row["Autoeficacia"] < 3.0:
        puntaje += 1
    if pd.notna(row["Emocional"]) and row["Emocional"] < 3.0:
        puntaje += 1
    if pd.notna(row["Autodeterminacion"]) and row["Autodeterminacion"] < 3.0:
        puntaje += 1
    if pd.notna(row["Sociabilidad"]) and row["Sociabilidad"] < 3.0:
        puntaje += 1
    if pd.notna(row["Prospectiva"]) and row["Prospectiva"] < 3.0:
        puntaje += 1

    X = pd.DataFrame([{
        "RamosReprobados": row["RamosReprobados"],
        "Autoeficacia": row["Autoeficacia"],
        "Emocional": row["Emocional"],
        "Autodeterminacion": row["Autodeterminacion"],
        "Sociabilidad": row["Sociabilidad"],
        "Prospectiva": row["Prospectiva"],
        "PuntajeHeuristico": puntaje
    }])

    pred = modelo.predict(X)[0]
    proba = modelo.predict_proba(X)[0]

    riesgo_str = ["Bajo", "Medio", "Alto"][pred]
    probabilidades = {
        "Bajo": round(proba[0], 2),
        "Medio": round(proba[1], 2),
        "Alto": round(proba[2], 2)
    }

    return {
        "rut": rut,
        "nombre": row["NOMBRE COMPLETO"],
        "carrera": row["Carrera"],
        "riesgo": riesgo_str,
        "clase": int(pred),
        "probabilidades": probabilidades
    }
