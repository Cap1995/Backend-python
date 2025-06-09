def combinar_niveles(riesgos: list[str]) -> str:
    puntaje_total = 0
    for r in riesgos:
        puntaje_total += {
            "Bajo": 1,
            "Medio": 2,
            "Alto": 3
        }.get(r, 0)

    if puntaje_total <= 4:
        return "Bajo"
    elif puntaje_total <= 6:
        return "Medio"
    else:
        return "Alto"
