import argparse
import itertools
import socket
import threading
import time

from protocolo import enviar, leer_mensajes

# Configuración de la conexión con el servidor (puerto de clientes)
HOST = "127.0.0.1"
PUERTO_CLIENTES = 5000

# Tareas de ejemplo para el modo demo: cada una tarda 1 segundo en el worker
LOTE_DEMO = [
    ("contar_palabras", {"texto": "programación sobre redes con sockets e hilos", "demora": 1}),
    ("invertir", {"texto": "sistema distribuido", "demora": 1}),
    ("mayusculas", {"texto": "balanceador de carga", "demora": 1}),
    ("es_primo", {"numero": 1000000007, "demora": 1}),
    ("fibonacci", {"n": 50, "demora": 1}),
    ("sumar", {"numeros": [10, 20, 30.5], "demora": 1}),
    ("contar_palabras", {"texto": "cola de mensajes", "demora": 1}),
    ("invertir", {"texto": "worker", "demora": 1}),
    ("es_primo", {"numero": 97, "demora": 1}),
    ("fibonacci", {"n": 10, "demora": 1}),
    ("mayusculas", {"texto": "pool de hilos", "demora": 1}),
    ("sumar", {"numeros": [1, 2, 3, 4, 5], "demora": 1}),
]


class Cliente:
    # Mantiene la conexión con el servidor. Las tareas se envían desde el hilo
    # principal y los resultados llegan en cualquier orden: los lee un hilo aparte
    def __init__(self, host, puerto):
        self.sock = socket.create_connection((host, puerto), timeout=5)
        self.sock.settimeout(None)
        self.lock = threading.Lock()
        self.refs = itertools.count(1)
        self.pendientes = set()
        self.cond = threading.Condition()
        self.resultados = []
        self.conectado = True
        threading.Thread(target=self.escuchar, daemon=True).start()

    def enviar_tarea(self, tarea, datos):
        ref = str(next(self.refs))
        with self.cond:
            self.pendientes.add(ref)
        enviar(self.sock, {"tipo": "tarea", "tarea": tarea, "datos": datos, "ref": ref}, self.lock)
        return ref

    def pedir_estado(self):
        enviar(self.sock, {"tipo": "estado"}, self.lock)

    def escuchar(self):
        # Hilo receptor: muestra cada mensaje del servidor a medida que llega
        try:
            for mensaje in leer_mensajes(self.sock):
                self.mostrar(mensaje)
        except OSError:
            pass
        self.conectado = False
        with self.cond:
            self.cond.notify_all()

    def mostrar(self, mensaje):
        tipo = mensaje.get("tipo")
        if tipo == "aceptada":
            print(f"  -> Tarea {mensaje['id']} aceptada (ref {mensaje['ref']}), tareas en cola: {mensaje['en_cola']}")
        elif tipo == "resultado":
            if mensaje["estado"] == "ok":
                detalle = f"resultado: {mensaje['resultado']}"
            else:
                detalle = f"error: {mensaje['error']}"
            print(f"  <- {mensaje['id']} {mensaje['tarea']} | {detalle} | {mensaje['worker']} ({mensaje['hilo']}) en {mensaje['duracion']} s")
            with self.cond:
                self.resultados.append(mensaje)
                self.pendientes.discard(mensaje.get("ref"))
                self.cond.notify_all()
        elif tipo == "estado":
            print(f"  Estado: {mensaje['en_cola']} en cola, {mensaje['procesadas']} procesadas")
            for w in mensaje["workers"]:
                print(f"    {w['nombre']}: {w['en_curso']} de {w['hilos']} hilos ocupados")
            if not mensaje["workers"]:
                print("    No hay workers conectados: las tareas esperan en la cola.")
        elif tipo == "error":
            print(f"  Error del servidor: {mensaje.get('detalle')}")
            with self.cond:
                self.pendientes.discard(mensaje.get("ref"))
                self.cond.notify_all()

    def esperar_resultados(self, limite=120):
        # Bloquea hasta recibir todos los resultados pendientes (o hasta el límite)
        fin = time.time() + limite
        with self.cond:
            while self.pendientes and self.conectado and time.time() < fin:
                self.cond.wait(timeout=0.5)
            return not self.pendientes

    def cerrar(self):
        self.sock.close()


def leer_datos(tarea):
    # Pide por consola los datos que necesita cada tipo de tarea
    if tarea in ("contar_palabras", "invertir", "mayusculas"):
        datos = {"texto": input("Texto: ")}
    elif tarea == "es_primo":
        datos = {"numero": int(input("Número: "))}
    elif tarea == "fibonacci":
        datos = {"n": int(input("Posición n (0 a 1000): "))}
    else:
        datos = {"numeros": [float(x) for x in input("Números separados por espacio: ").split()]}
    demora = input("Demora simulada en segundos (Enter = 0): ").strip()
    if demora:
        datos["demora"] = float(demora)
    return datos


def ejecutar_demo(cliente):
    print(f"Enviando {len(LOTE_DEMO)} tareas de 1 segundo cada una...")
    inicio = time.perf_counter()
    for tarea, datos in LOTE_DEMO:
        cliente.enviar_tarea(tarea, datos)
    completo = cliente.esperar_resultados()
    duracion = time.perf_counter() - inicio
    usados = sorted({r["worker"] for r in cliente.resultados})
    print(f"\n{len(cliente.resultados)} resultados en {duracion:.2f} s "
          f"(en serie serían unos {len(LOTE_DEMO)} s). Workers que participaron: {', '.join(usados) or 'ninguno'}")
    if not completo:
        print("Atención: no llegaron todos los resultados.")


def menu(cliente):
    tipos = ["contar_palabras", "invertir", "mayusculas", "es_primo", "fibonacci", "sumar"]
    while cliente.conectado:
        print("\n1. Enviar una tarea\n2. Enviar lote de demostración\n3. Ver estado del sistema\n4. Salir")
        opcion = input("Opción: ").strip()
        if opcion == "1":
            for i, t in enumerate(tipos, 1):
                print(f"  {i}. {t}")
            eleccion = input("Tipo de tarea: ").strip()
            if not eleccion.isdigit() or not 1 <= int(eleccion) <= len(tipos):
                print("Tipo inválido.")
                continue
            tarea = tipos[int(eleccion) - 1]
            try:
                datos = leer_datos(tarea)
            except ValueError:
                print("Dato inválido: se esperaba un número.")
                continue
            cliente.enviar_tarea(tarea, datos)
            cliente.esperar_resultados(limite=30)
        elif opcion == "2":
            cliente.resultados.clear()
            ejecutar_demo(cliente)
        elif opcion == "3":
            cliente.pedir_estado()
            time.sleep(0.3)
        elif opcion == "4":
            break
        else:
            print("Opción inválida.")
    if not cliente.conectado:
        print("Se perdió la conexión con el servidor.")


if __name__ == "__main__":
    parametros = argparse.ArgumentParser(description="Cliente del sistema distribuido de tareas")
    parametros.add_argument("--demo", action="store_true", help="envía el lote de demostración y termina")
    argumentos = parametros.parse_args()
    try:
        cliente = Cliente(HOST, PUERTO_CLIENTES)
    except OSError:
        print(f"No se pudo conectar al servidor en {HOST}:{PUERTO_CLIENTES}. Verificá que esté en ejecución.")
        raise SystemExit(1)
    try:
        if argumentos.demo:
            ejecutar_demo(cliente)
        else:
            menu(cliente)
    except KeyboardInterrupt:
        print("\nCliente detenido.")
    finally:
        cliente.cerrar()
