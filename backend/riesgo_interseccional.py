def calcular_riesgo_interseccional(vulnerabilidades: list[int]):
    """
    Calcula el riesgo dado un listado de factores

    Poderaciones por posicion(ejemplo):
    0: Identidad de género (1)
    1: Primera generación (2)
    2: Nivel socioeconómico (3)
    3: Salud mental (2)
    4: Discapacidad (3)
    5: Edad >= 25 (1)
    6: Trabajador (2)
    7: Padre/madre (2)
    """
    ponderaciones = [1, 2, 3, 2, 3, 1, 2, 2]
    puntaje = sum(v * p for v, p in zip(vulnerabilidades, ponderaciones))

    #Nivel de riesgo

    if puntaje >= 8:
        nivel= "Alto"
    elif puntaje >= 5:
        nivel = "Medio"
    else:
        nivel = "Bajo"

    
    # Detalle (opcional: lista de qué factores aportaron)
    factores_activados = [i for i, v in enumerate(vulnerabilidades) if v == 1]
    detalle = f"Factores activados: {factores_activados}"

    return puntaje, nivel, detalle