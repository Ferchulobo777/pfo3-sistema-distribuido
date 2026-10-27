import json

# Protocolo de aplicación compartido por servidor, workers y clientes.
# Cada mensaje es un objeto JSON en una sola línea terminada en "\n" (JSON Lines).
# El salto de línea delimita los mensajes: TCP es un flujo de bytes y recv()
# puede entregar un mensaje partido en varios bloques o dos mensajes juntos.

CODIFICACION = "utf-8"


def enviar(sock, mensaje, lock=None):
    # Serializa el mensaje y lo envía completo. Si varios hilos escriben en el
    # mismo socket se pasa un lock para que las líneas no se mezclen
    datos = (json.dumps(mensaje, ensure_ascii=False) + "\n").encode(CODIFICACION)
    if lock is None:
        sock.sendall(datos)
    else:
        with lock:
            sock.sendall(datos)


def leer_mensajes(sock):
    # Generador que devuelve un diccionario por cada línea recibida.
    # makefile() junta los bloques de recv() hasta completar cada línea.
    # Termina cuando el otro extremo cierra la conexión
    with sock.makefile("r", encoding=CODIFICACION) as lector:
        for linea in lector:
            linea = linea.strip()
            if not linea:
                continue
            try:
                mensaje = json.loads(linea)
            except json.JSONDecodeError:
                yield {"tipo": "invalido", "detalle": "El mensaje no es JSON válido"}
                continue
            if isinstance(mensaje, dict):
                yield mensaje
            else:
                yield {"tipo": "invalido", "detalle": "Se esperaba un objeto JSON"}
