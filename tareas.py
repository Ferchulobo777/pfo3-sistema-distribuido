import time

# Catálogo de tareas que saben ejecutar los workers.
# Cada función recibe el diccionario "datos" de la tarea y devuelve el resultado.
# El parámetro opcional "demora" (segundos, máximo 10) simula una tarea larga
# y sirve para observar el procesamiento en paralelo.

DEMORA_MAXIMA = 10


def _esperar(datos):
    demora = datos.get("demora", 0)
    if not isinstance(demora, (int, float)) or demora < 0:
        raise ValueError("'demora' debe ser un número mayor o igual a 0")
    time.sleep(min(demora, DEMORA_MAXIMA))


def _texto(datos):
    texto = datos.get("texto")
    if not isinstance(texto, str):
        raise ValueError("Falta el campo 'texto' (cadena)")
    return texto


def _entero(datos, campo, minimo, maximo):
    valor = datos.get(campo)
    if not isinstance(valor, int) or isinstance(valor, bool) or not minimo <= valor <= maximo:
        raise ValueError(f"'{campo}' debe ser un entero entre {minimo} y {maximo}")
    return valor


def contar_palabras(datos):
    _esperar(datos)
    texto = _texto(datos)
    return {"palabras": len(texto.split()), "caracteres": len(texto)}


def invertir(datos):
    _esperar(datos)
    return _texto(datos)[::-1]


def mayusculas(datos):
    _esperar(datos)
    return _texto(datos).upper()


def es_primo(datos):
    _esperar(datos)
    n = _entero(datos, "numero", 0, 10**12)
    if n < 2:
        return False
    if n % 2 == 0:
        return n == 2
    divisor = 3
    while divisor * divisor <= n:
        if n % divisor == 0:
            return False
        divisor += 2
    return True


def fibonacci(datos):
    _esperar(datos)
    n = _entero(datos, "n", 0, 1000)
    a, b = 0, 1
    for _ in range(n):
        a, b = b, a + b
    return a


def sumar(datos):
    _esperar(datos)
    numeros = datos.get("numeros")
    if not isinstance(numeros, list) or not all(isinstance(x, (int, float)) and not isinstance(x, bool) for x in numeros):
        raise ValueError("'numeros' debe ser una lista de números")
    return sum(numeros)


CATALOGO = {
    "contar_palabras": contar_palabras,
    "invertir": invertir,
    "mayusculas": mayusculas,
    "es_primo": es_primo,
    "fibonacci": fibonacci,
    "sumar": sumar,
}
