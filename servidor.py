import datetime
import itertools
import json
import queue
import socket
import sqlite3
import threading
import time

from protocolo import enviar, leer_mensajes
from tareas import CATALOGO

# Configuración de red: un puerto para los clientes y otro para los workers
HOST = "127.0.0.1"
PUERTO_CLIENTES = 5000
PUERTO_WORKERS = 5001
DB_NAME = "tareas.db"

# Cola central de tareas pendientes. Cumple el rol de la cola de mensajes
# (RabbitMQ en el diagrama): los clientes publican y los workers consumen.
# queue.Queue ya es segura para usar desde varios hilos
cola_tareas = queue.Queue()

# Tareas enviadas a un worker que todavía no devolvieron resultado: id -> tarea
tareas_en_curso = {}
lock_en_curso = threading.Lock()

# Workers conectados: nombre -> Worker
workers = {}
lock_workers = threading.Lock()

# Generador de identificadores únicos de tarea (T0001, T0002, ...)
contador_ids = itertools.count(1)
lock_ids = threading.Lock()

# SQLite admite un solo escritor a la vez: se serializan las escrituras
lock_bd = threading.Lock()

procesadas = 0


class ConexionCliente:
    # Socket de un cliente y el lock que evita que dos hilos escriban a la vez
    # (el hilo del cliente confirma tareas y los hilos de los workers devuelven resultados)
    def __init__(self, sock, direccion):
        self.sock = sock
        self.direccion = f"{direccion[0]}:{direccion[1]}"
        self.lock = threading.Lock()
        self.conectado = True

    def responder(self, mensaje):
        if not self.conectado:
            return
        try:
            enviar(self.sock, mensaje, self.lock)
        except OSError:
            self.conectado = False


class Worker:
    # Nodo de procesamiento conectado al servidor. El semáforo vale la cantidad
    # de hilos del pool del worker: nunca se le envían más tareas que hilos libres
    def __init__(self, sock, nombre, hilos):
        self.sock = sock
        self.nombre = nombre
        self.hilos = hilos
        self.semaforo = threading.Semaphore(hilos)
        self.lock = threading.Lock()
        self.activo = True
        self.en_curso = set()


def registrar(texto):
    hora = datetime.datetime.now().strftime("%H:%M:%S")
    print(f"[{hora}] {texto}", flush=True)


def nuevo_id():
    with lock_ids:
        return f"T{next(contador_ids):04d}"


# Persistencia ---------------------------------------------------------------

def inicializar_bd():
    # Crea la tabla de tareas. En el diagrama este rol lo cumple PostgreSQL
    conexion = sqlite3.connect(DB_NAME)
    try:
        with conexion:
            conexion.execute("""
                CREATE TABLE IF NOT EXISTS tareas (
                    id TEXT PRIMARY KEY,
                    tipo TEXT NOT NULL,
                    datos TEXT NOT NULL,
                    estado TEXT NOT NULL,
                    resultado TEXT,
                    error TEXT,
                    worker TEXT,
                    cliente TEXT NOT NULL,
                    recibida TEXT NOT NULL,
                    finalizada TEXT
                )
            """)
    finally:
        conexion.close()


def ejecutar_bd(sql, parametros):
    # Abre una conexión por operación (sqlite3 no comparte conexiones entre hilos)
    with lock_bd:
        conexion = sqlite3.connect(DB_NAME)
        try:
            with conexion:
                conexion.execute(sql, parametros)
        except sqlite3.Error as error:
            registrar(f"Error al escribir en la base de datos: {error}")
        finally:
            conexion.close()


def ahora():
    return datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


# Atención de clientes -------------------------------------------------------

def recibir_tarea(cliente, mensaje):
    # Valida la tarea, la guarda como pendiente y la publica en la cola
    tipo = mensaje.get("tarea")
    datos = mensaje.get("datos", {})
    ref = mensaje.get("ref")
    if tipo not in CATALOGO:
        cliente.responder({"tipo": "error", "ref": ref,
                           "detalle": f"Tarea desconocida. Disponibles: {', '.join(CATALOGO)}"})
        return
    if not isinstance(datos, dict):
        cliente.responder({"tipo": "error", "ref": ref, "detalle": "'datos' debe ser un objeto JSON"})
        return

    tarea = {"id": nuevo_id(), "ref": ref, "tarea": tipo, "datos": datos, "cliente": cliente}
    ejecutar_bd("INSERT INTO tareas (id, tipo, datos, estado, cliente, recibida) VALUES (?, ?, ?, ?, ?, ?)",
                (tarea["id"], tipo, json.dumps(datos, ensure_ascii=False), "pendiente", cliente.direccion, ahora()))
    cola_tareas.put(tarea)
    cliente.responder({"tipo": "aceptada", "id": tarea["id"], "ref": ref, "en_cola": cola_tareas.qsize()})
    registrar(f"Tarea {tarea['id']} ({tipo}) recibida de {cliente.direccion}, en cola: {cola_tareas.qsize()}")


def estado_sistema():
    with lock_workers:
        lista = [{"nombre": w.nombre, "hilos": w.hilos, "en_curso": len(w.en_curso)} for w in workers.values()]
    return {"tipo": "estado", "workers": lista, "en_cola": cola_tareas.qsize(), "procesadas": procesadas}


def atender_cliente(sock, direccion):
    # Un hilo por cliente: lee sus mensajes y responde. Los resultados le llegan
    # más tarde, desde los hilos que atienden a los workers
    cliente = ConexionCliente(sock, direccion)
    registrar(f"Cliente conectado: {cliente.direccion}")
    try:
        for mensaje in leer_mensajes(sock):
            tipo = mensaje.get("tipo")
            if tipo == "tarea":
                recibir_tarea(cliente, mensaje)
            elif tipo == "estado":
                cliente.responder(estado_sistema())
            elif tipo == "invalido":
                cliente.responder({"tipo": "error", "detalle": mensaje["detalle"]})
            else:
                cliente.responder({"tipo": "error", "detalle": "Tipo de mensaje no reconocido"})
    except OSError:
        pass
    finally:
        cliente.conectado = False
        sock.close()
        registrar(f"Cliente desconectado: {cliente.direccion}")


# Atención de workers --------------------------------------------------------

def despachar(worker):
    # Hilo despachador de un worker: espera a que tenga un hilo libre, toma la
    # siguiente tarea de la cola y se la envía. Como todos los despachadores
    # consumen de la misma cola, cada tarea va al worker que primero se libera
    while worker.activo:
        if not worker.semaforo.acquire(timeout=0.5):
            continue
        tarea = None
        while worker.activo and tarea is None:
            try:
                tarea = cola_tareas.get(timeout=0.5)
            except queue.Empty:
                pass
        if tarea is None:
            worker.semaforo.release()
            break

        # El chequeo de "activo" y el alta en curso van juntos bajo el lock: si el
        # worker se desconectó en este instante, la tarea vuelve a la cola en lugar
        # de perderse (reencolar_tareas usa el mismo lock)
        with lock_en_curso:
            if not worker.activo:
                cola_tareas.put(tarea)
                break
            tareas_en_curso[tarea["id"]] = tarea
            worker.en_curso.add(tarea["id"])
        # El estado se marca antes de enviar: si se marcara después, un worker rápido
        # podría devolver el resultado primero y "en_proceso" pisaría "completada"
        ejecutar_bd("UPDATE tareas SET estado = ?, worker = ? WHERE id = ?", ("en_proceso", worker.nombre, tarea["id"]))
        try:
            enviar(worker.sock, {"tipo": "tarea", "id": tarea["id"], "tarea": tarea["tarea"], "datos": tarea["datos"]}, worker.lock)
            registrar(f"Tarea {tarea['id']} enviada a {worker.nombre}")
        except OSError:
            worker.activo = False
            break


def recibir_resultado(worker, mensaje):
    # Registra el resultado de una tarea y se lo reenvía al cliente que la pidió
    global procesadas
    id_tarea = mensaje.get("id")
    with lock_en_curso:
        tarea = tareas_en_curso.pop(id_tarea, None)
        worker.en_curso.discard(id_tarea)
        if tarea is not None:
            procesadas += 1
    if tarea is None:
        return
    worker.semaforo.release()

    estado = "ok" if mensaje.get("estado") == "ok" else "error"
    ejecutar_bd("UPDATE tareas SET estado = ?, resultado = ?, error = ?, finalizada = ? WHERE id = ?",
                ("completada" if estado == "ok" else "fallida",
                 json.dumps(mensaje.get("resultado"), ensure_ascii=False), mensaje.get("error"), ahora(), id_tarea))
    tarea["cliente"].responder({
        "tipo": "resultado", "id": id_tarea, "ref": tarea["ref"], "tarea": tarea["tarea"],
        "estado": estado, "resultado": mensaje.get("resultado"), "error": mensaje.get("error"),
        "worker": worker.nombre, "hilo": mensaje.get("hilo"), "duracion": mensaje.get("duracion"),
    })
    registrar(f"Tarea {id_tarea} terminada por {worker.nombre} ({estado})")


def reencolar_tareas(worker):
    # Tolerancia a fallos: si un worker se desconecta, sus tareas sin terminar
    # vuelven a la cola para que las procese otro worker
    with lock_en_curso:
        pendientes = [tareas_en_curso.pop(i) for i in list(worker.en_curso) if i in tareas_en_curso]
        worker.en_curso.clear()
    for tarea in pendientes:
        ejecutar_bd("UPDATE tareas SET estado = ?, worker = NULL WHERE id = ?", ("pendiente", tarea["id"]))
        cola_tareas.put(tarea)
    if pendientes:
        registrar(f"{len(pendientes)} tarea(s) de {worker.nombre} devueltas a la cola: {', '.join(t['id'] for t in pendientes)}")


def atender_worker(sock, direccion):
    # Primer mensaje: registro con nombre y cantidad de hilos del pool.
    # Después: un hilo despachador envía tareas y este hilo recibe resultados
    mensajes = leer_mensajes(sock)
    try:
        registro = next(mensajes)
    except (StopIteration, OSError):
        sock.close()
        return
    hilos = registro.get("hilos")
    if registro.get("tipo") != "registro" or not isinstance(hilos, int) or not 1 <= hilos <= 64:
        enviar(sock, {"tipo": "error", "detalle": "Se esperaba {'tipo': 'registro', 'nombre': ..., 'hilos': 1 a 64}"})
        sock.close()
        return

    with lock_workers:
        nombre = str(registro.get("nombre") or f"worker-{direccion[1]}")
        if nombre in workers:
            nombre = f"{nombre}-{direccion[1]}"
        worker = Worker(sock, nombre, hilos)
        workers[nombre] = worker
    enviar(sock, {"tipo": "registrado", "nombre": nombre}, worker.lock)
    registrar(f"Worker {nombre} conectado desde {direccion[0]}:{direccion[1]} con {hilos} hilos")
    threading.Thread(target=despachar, args=(worker,), name=f"despachador-{nombre}", daemon=True).start()

    try:
        for mensaje in mensajes:
            if mensaje.get("tipo") == "resultado":
                recibir_resultado(worker, mensaje)
    except OSError:
        pass
    finally:
        with lock_en_curso:
            worker.activo = False
        with lock_workers:
            workers.pop(nombre, None)
        sock.close()
        registrar(f"Worker {nombre} desconectado")
        reencolar_tareas(worker)


# Arranque -------------------------------------------------------------------

def crear_socket(puerto):
    # Sin SO_REUSEADDR: en Windows esa opción deja que dos procesos escuchen
    # el mismo puerto sin error, y taparía el control de puerto ocupado
    servidor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    servidor.bind((HOST, puerto))
    servidor.listen()
    return servidor


def aceptar_conexiones(servidor, funcion, rol):
    # Bucle de accept(): cada conexión nueva se atiende en su propio hilo
    while True:
        try:
            sock, direccion = servidor.accept()
        except OSError:
            break
        threading.Thread(target=funcion, args=(sock, direccion), name=f"{rol}-{direccion[1]}", daemon=True).start()


def iniciar_servidor():
    try:
        inicializar_bd()
    except sqlite3.Error as error:
        print(f"No se pudo acceder a la base de datos: {error}")
        return

    try:
        socket_clientes = crear_socket(PUERTO_CLIENTES)
        socket_workers = crear_socket(PUERTO_WORKERS)
    except OSError:
        print(f"No se pudo iniciar el servidor: los puertos {PUERTO_CLIENTES} o {PUERTO_WORKERS} ya están en uso.")
        return

    threading.Thread(target=aceptar_conexiones, args=(socket_clientes, atender_cliente, "cliente"), daemon=True).start()
    threading.Thread(target=aceptar_conexiones, args=(socket_workers, atender_worker, "worker"), daemon=True).start()
    registrar(f"Servidor escuchando clientes en {HOST}:{PUERTO_CLIENTES} y workers en {HOST}:{PUERTO_WORKERS}")

    # El hilo principal solo espera Ctrl+C (en Windows accept() no lo deja pasar)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        registrar("Servidor detenido manualmente.")
    finally:
        socket_clientes.close()
        socket_workers.close()


if __name__ == "__main__":
    iniciar_servidor()
