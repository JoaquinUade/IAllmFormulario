"""Lógica de negocio para generar el resumen comercial."""

from __future__ import annotations

from app.schemas import ClientePotencial, Permuta, Ubicacion


def formato_pesos(valor: int) -> str:
    return f"${valor:,}".replace(",", ".")


def texto_ubicacion(ubicacion: Ubicacion | None) -> str:
    if ubicacion is None:
        return "No informada"

    partes = [
        parte.replace("_", " ")
        for parte in (ubicacion.localidad, ubicacion.provincia)
        if parte
    ]
    return ", ".join(partes) if partes else "No informada"


def texto_permuta(permuta: Permuta | None) -> str:
    if permuta is None:
        return "No informada"
    if not permuta.tienePermuta:
        return "No"

    detalles = ["Sí"]
    if permuta.descripcion:
        detalles.append(permuta.descripcion)
    if permuta.valorDeclarado is not None:
        detalles.append(f"valor declarado {formato_pesos(permuta.valorDeclarado)}")
    return ", ".join(detalles)


def generar_resumen(datos: ClientePotencial) -> str:
    nombre = f"{datos.cliente.nombre} {datos.cliente.apellido}".strip()
    vehiculo = (
        f"{datos.vehiculo.marca} {datos.vehiculo.modelo} "
        f"({datos.vehiculo.anio})"
    )
    anticipo = (
        formato_pesos(datos.anticipo)
        if datos.anticipo is not None
        else "No informado"
    )
    financiacion = (
        "Sí"
        if datos.solicitaFinanciacion is True
        else "No"
        if datos.solicitaFinanciacion is False
        else "No informada"
    )

    conclusion = (
        "El asesor debería validar la tasación de la permuta y revisar las "
        "condiciones de financiación solicitadas."
        if datos.permuta is not None and datos.permuta.tienePermuta
        and datos.solicitaFinanciacion is True
        else "El asesor debería confirmar los datos comerciales y las "
        "condiciones de la operación."
    )

    return "\n".join(
        [
            f"Cliente: {nombre}.",
            f"Vehículo seleccionado: {vehiculo}.",
            f"Ubicación: {texto_ubicacion(datos.ubicacion)}. Anticipo: {anticipo}.",
            f"Permuta: {texto_permuta(datos.permuta)}. Financiación: {financiacion}.",
            f"Conclusión comercial: {conclusion}",
        ]
    )
