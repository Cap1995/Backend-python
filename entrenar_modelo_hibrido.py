import pandas as pd
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import train_test_split
from sklearn.metrics import classification_report
import joblib

# Cargar el dataset
df = pd.read_csv("dataset_hibrido.csv")

# Validar columnas necesarias
columnas_obligatorias = [
    "RamosReprobados", "Autoeficacia", "Emocional",
    "Autodeterminacion", "Sociabilidad", "Prospectiva",
    "PuntajeHeuristico", "NivelHeuristico"
]
if not all(col in df.columns for col in columnas_obligatorias):
    print("❌ Faltan columnas necesarias en el dataset. Revisa el archivo CSV.")
    exit()

# Limpiar filas con valores nulos
df = df.dropna(subset=columnas_obligatorias)

# Separar variables
X = df[[
    "RamosReprobados",
    "Autoeficacia",
    "Emocional",
    "Autodeterminacion",
    "Sociabilidad",
    "Prospectiva",
    "PuntajeHeuristico"
]]
y = df["NivelHeuristico"]  # 0 = Bajo, 1 = Medio, 2 = Alto

# Dividir en entrenamiento y prueba
X_train, X_test, y_train, y_test = train_test_split(
    X, y, test_size=0.2, random_state=42, stratify=y
)

# Entrenar modelo
modelo = RandomForestClassifier(n_estimators=100, random_state=42)
modelo.fit(X_train, y_train)

# Evaluar
y_pred = modelo.predict(X_test)
print("🧪 Evaluación del modelo:")
print(classification_report(y_test, y_pred, digits=3))

# Guardar modelo
joblib.dump(modelo, "modelo_hibrido.pkl")
print("✅ Modelo guardado como modelo_hibrido.pkl")
