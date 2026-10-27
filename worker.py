import argparse
import datetime
import socket
import threading
import time
from concurrent.futures import ThreadPoolExecutor

from protocolo import enviar, leer_mensajes
from tareas import CATALOGO

# Configuración de la conexión con el servidor (puerto de workers)
HOST = "127.0.0.1"
PUERTO_WORKERS = 5001


def registrar(nombre, texto):
    hora = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{hora}] [{nombre}] {texto}", flush=True)


def procesar(sock, lock, nombre, mensaje):
    # Se ejecuta en un hilo del pool: resuelve la tarea y envía el resultado.
    # Un error en la tarea se devuelve como resultado con estado "error"
    # para que el cliente se entere y el worker siga funcionando
    inicio = time.perf_counter()
    hilo = threading.current_thread().name
    funcion = CATALOGO.get(mensaje.get("tarea"))
    respuesta = {"tipo": "resultado", "id": mensaje.get("id"), "hilo": hilo}
    try:
        if funcion is None:
            raise ValueError("Tarea desconocida")
        respuesta.update(estado="ok", resultado=funcion(mensaje.get("datos") or {}))
    except Exception as error:
        respuesta.update(estado="error", error=str(error))
    respuesta["duracion"] = round(time.perf_counter() - inicio, 3)
    registrar(nombre, f"{hilo} terminó {mensaje.get('id')} ({respuesta['estado']}, {respuesta['duracion']} s)")
    try:
        enviar(sock, respuesta, lock)
    except OSError:
        registrar(nombre, "No se pudo enviar el resultado: se perdió la conexión con el servidor")


def conectar(host, puerto, intentos=5):
    # Reintenta con espera creciente por si el servidor todavía no arrancó
    espera = 1
    for intento in range(1, intentos + 1):
        try:
            return socket.create_connection((host, puerto), timeout=5)
        except OSError:
            if intento == intentos:
                return None
            time.sleep(espera)
            espera = min(espera * 2, 8)


def iniciar_worker(nombre, hilos, host=HOST, puerto=PUERTO_WORKERS):
    sock = conectar(host, puerto)
    if sock is None:
        print(f"No se pudo conectar al servidor en {host}:{puerto}. Verificá que esté en ejecución.")
        return
    sock.settimeout(None)
    lock = threading.Lock()

    # Pool de hilos del worker: procesa hasta "hilos" tareas a la vez.
    # El servidor no le envía más tareas que hilos disponibles
    with sock, ThreadPoolExecutor(max_workers=hilos, thread_name_prefix=f"{nombre}-hilo") as pool:
        enviar(sock, {"tipo": "registro", "nombre": nombre, "hilos": hilos}, lock)
        try:
            for mensaje in leer_mensajes(sock):
                tipo = mensaje.get("tipo")
                if tipo == "registrado":
                    nombre = mensaje.get("nombre", nombre)
                    registrar(nombre, f"Registrado en {host}:{puerto} con un pool de {hilos} hilos")
                elif tipo == "tarea":
                    registrar(nombre, f"Recibió {mensaje.get('id')} ({mensaje.get('tarea')})")
                    pool.submit(procesar, sock, lock, nombre, mensaje)
                elif tipo == "error":
                    registrar(nombre, f"El servidor rechazó el registro: {mensaje.get('detalle')}")
                    break
        except (ConnectionResetError, ConnectionAbortedError):
            pass
        except KeyboardInterrupt:
            registrar(nombre, "Worker detenido manualmente.")
            return
    registrar(nombre, "Conexión con el servidor cerrada.")


if __name__ == "__main__":
    parametros = argparse.ArgumentParser(description="Worker del sistema distribuido de tareas")
    parametros.add_argument("--nombre", default=f"worker-{socket.gethostname()}")
    parametros.add_argument("--hilos", type=int, default=4, help="tamaño del pool de hilos (1 a 64)")
    argumentos = parametros.parse_args()
    if not 1 <= argumentos.hilos <= 64:
        parametros.error("--hilos debe estar entre 1 y 64")
    try:
        iniciar_worker(argumentos.nombre, argumentos.hilos)
    except KeyboardInterrupt:
        print("Worker detenido manualmente.")
