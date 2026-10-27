"""Pruebas de extremo a extremo del sistema distribuido.

Levanta procesos reales (servidor, workers y clientes) sobre una copia del
proyecto en una carpeta temporal, para empezar siempre con una base vacía.
Uso: python pruebas/prueba_sistema.py
"""
import json
import os
import shutil
import socket
import sqlite3
import subprocess
import sys
import tempfile
import threading
import time

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
ARCHIVOS = ["servidor.py", "worker.py", "cliente.py", "protocolo.py", "tareas.py"]
HOST, PUERTO_CLIENTES = "127.0.0.1", 5000
ENTORNO = dict(os.environ, PYTHONIOENCODING="utf-8", PYTHONUNBUFFERED="1")

resultados = []


def comprobar(nombre, condicion, detalle=""):
    resultados.append(condicion)
    print(f"[{'OK' if condicion else 'FALLA'}] {nombre}" + (f": {detalle}" if detalle else ""), flush=True)


class Conexion:
    # Cliente mínimo para las pruebas: envía mensajes y guarda lo que recibe
    def __init__(self):
        self.sock = socket.create_connection((HOST, PUERTO_CLIENTES), timeout=5)
        self.sock.settimeout(None)
        self.recibidos = []
        self.cond = threading.Condition()
        threading.Thread(target=self._leer, daemon=True).start()

    def _leer(self):
        try:
            with self.sock.makefile("r", encoding="utf-8") as lector:
                for linea in lector:
                    with self.cond:
                        self.recibidos.append(json.loads(linea))
                        self.cond.notify_all()
        except (OSError, ValueError):
            pass  # la prueba cerró el socket

    def enviar(self, mensaje):
        datos = mensaje if isinstance(mensaje, bytes) else (json.dumps(mensaje) + "\n").encode("utf-8")
        self.sock.sendall(datos)

    def tarea(self, tarea, datos, ref):
        self.enviar({"tipo": "tarea", "tarea": tarea, "datos": datos, "ref": ref})

    def esperar(self, condicion, limite=20):
        fin = time.time() + limite
        with self.cond:
            while not condicion(self.recibidos) and time.time() < fin:
                self.cond.wait(timeout=0.2)
            return condicion(self.recibidos)

    def de_tipo(self, tipo):
        return [m for m in self.recibidos if m.get("tipo") == tipo]

    def cerrar(self):
        self.sock.close()


def lanzar(carpeta, *argumentos):
    # La salida va a un archivo: con un PIPE que nadie lee, el buffer se llena,
    # print() se bloquea y el proceso se congela a mitad de la prueba
    nombre = argumentos[2] if "--nombre" in argumentos else os.path.splitext(argumentos[0])[0]
    log = open(os.path.join(carpeta, f"{nombre}.log"), "w", encoding="utf-8")
    proceso = subprocess.Popen([sys.executable, *argumentos], cwd=carpeta, env=ENTORNO,
                               stdout=log, stderr=subprocess.STDOUT)
    proceso.log = log
    return proceso


def detener(proceso):
    proceso.kill()
    proceso.wait()


def contar(cantidad):
    return lambda recibidos: len([m for m in recibidos if m.get("tipo") == "resultado"]) >= cantidad


def main():
    carpeta = tempfile.mkdtemp(prefix="pfo3_")
    for archivo in ARCHIVOS:
        shutil.copy(os.path.join(RAIZ, archivo), carpeta)
    procesos = []
    try:
        # 1. Cliente sin servidor
        r = subprocess.run([sys.executable, "cliente.py", "--demo"], cwd=carpeta, env=ENTORNO,
                           capture_output=True, text=True, encoding="utf-8", timeout=30)
        comprobar("Cliente sin servidor informa el error", "No se pudo conectar" in r.stdout, r.stdout.strip())

        servidor = lanzar(carpeta, "servidor.py")
        procesos.append(servidor)
        time.sleep(1.5)

        # 2. Puerto ocupado
        r = subprocess.run([sys.executable, "servidor.py"], cwd=carpeta, env=ENTORNO,
                           capture_output=True, text=True, encoding="utf-8", timeout=30)
        comprobar("Segundo servidor detecta el puerto ocupado", "ya están en uso" in r.stdout, r.stdout.strip())

        # 3. Sin workers la tarea queda en cola y se procesa cuando aparece uno
        c = Conexion()
        c.tarea("invertir", {"texto": "hola"}, "cola")
        time.sleep(1.5)
        comprobar("Sin workers la tarea queda en cola", c.esperar(lambda r: any(m.get("tipo") == "aceptada" for m in r), 5)
                  and not c.de_tipo("resultado"))
        w1 = lanzar(carpeta, "worker.py", "--nombre", "worker-1", "--hilos", "3")
        procesos.append(w1)
        ok = c.esperar(contar(1))
        primero = c.de_tipo("resultado")[0] if c.de_tipo("resultado") else {}
        comprobar("Al conectarse un worker procesa la tarea pendiente", ok and primero.get("resultado") == "aloh",
                  f"{primero.get('id')} -> {primero.get('resultado')!r} por {primero.get('worker')}")
        c.cerrar()

        # 4. Distribución y paralelismo: 12 tareas de 1 s entre 2 workers de 3 hilos
        w2 = lanzar(carpeta, "worker.py", "--nombre", "worker-2", "--hilos", "3")
        procesos.append(w2)
        time.sleep(1.5)
        r = subprocess.run([sys.executable, "cliente.py", "--demo"], cwd=carpeta, env=ENTORNO,
                           capture_output=True, text=True, encoding="utf-8", timeout=60)
        resumen = [l for l in r.stdout.splitlines() if "resultados en" in l]
        comprobar("Lote demo: 12 resultados", bool(resumen) and resumen[0].startswith("12 resultados"), resumen[0] if resumen else r.stdout[-300:])
        segundos = float(resumen[0].split(" en ")[1].split(" s")[0]) if resumen else 99
        comprobar("Lote demo en paralelo (12 tareas de 1 s en menos de 3,5 s)", segundos < 3.5, f"{segundos} s")
        comprobar("Lote demo repartido entre los dos workers", "worker-1" in r.stdout and "worker-2" in r.stdout)
        with open(os.path.join(RAIZ, "pruebas", "salida_demo.txt"), "w", encoding="utf-8") as archivo:
            archivo.write(r.stdout)

        # 5. Resultados correctos de cada tipo de tarea
        c = Conexion()
        casos = [("contar_palabras", {"texto": "uno dos tres"}, {"palabras": 3, "caracteres": 12}),
                 ("mayusculas", {"texto": "redes"}, "REDES"),
                 ("es_primo", {"numero": 1000000007}, True),
                 ("es_primo", {"numero": 1000000008}, False),
                 ("fibonacci", {"n": 50}, 12586269025),
                 ("sumar", {"numeros": [1, 2, 3.5]}, 6.5)]
        for i, (tarea, datos, _) in enumerate(casos):
            c.tarea(tarea, datos, f"caso{i}")
        c.esperar(contar(len(casos)))
        por_ref = {m["ref"]: m for m in c.de_tipo("resultado")}
        correctos = all(por_ref.get(f"caso{i}", {}).get("resultado") == esperado for i, (_, _, esperado) in enumerate(casos))
        comprobar("Cada tipo de tarea devuelve el resultado correcto", correctos)

        # 6. Errores: tarea desconocida, datos inválidos, JSON roto y tarea que falla
        c.tarea("borrar_todo", {}, "e1")
        c.enviar({"tipo": "tarea", "tarea": "invertir", "datos": [1, 2], "ref": "e2"})
        c.enviar(b"esto no es json\n")
        c.tarea("fibonacci", {"n": -5}, "e3")
        c.esperar(lambda r: len([m for m in r if m.get("tipo") == "error"]) >= 3 and any(m.get("ref") == "e3" and m.get("tipo") == "resultado" for m in r))
        errores = c.de_tipo("error")
        comprobar("Tarea desconocida rechazada", any(m.get("ref") == "e1" and "desconocida" in m["detalle"] for m in errores))
        comprobar("Datos que no son objeto rechazados", any(m.get("ref") == "e2" for m in errores))
        comprobar("JSON inválido informado sin cortar la conexión", any("JSON" in m["detalle"] for m in errores))
        e3 = [m for m in c.de_tipo("resultado") if m.get("ref") == "e3"]
        comprobar("Error dentro de la tarea vuelve como estado error", bool(e3) and e3[0]["estado"] == "error", e3[0]["error"] if e3 else "")
        c.tarea("invertir", {"texto": "sigue"}, "post")
        comprobar("El worker sigue funcionando después del error",
                  c.esperar(lambda r: any(m.get("ref") == "post" and m.get("tipo") == "resultado" for m in r)))

        # 7. Mensaje mucho más largo que el buffer de 1024 bytes
        texto = "palabra " * 5000
        c.tarea("contar_palabras", {"texto": texto}, "largo")
        c.esperar(lambda r: any(m.get("ref") == "largo" and m.get("tipo") == "resultado" for m in r))
        largo = [m for m in c.de_tipo("resultado") if m.get("ref") == "largo"]
        comprobar("Mensaje de 40 000 caracteres procesado completo", bool(largo) and largo[0]["resultado"]["palabras"] == 5000)

        # 8. Consulta de estado
        c.enviar({"tipo": "estado"})
        c.esperar(lambda r: any(m.get("tipo") == "estado" for m in r), 5)
        estado = c.de_tipo("estado")
        comprobar("Estado informa 2 workers con 3 hilos", bool(estado) and sorted(w["hilos"] for w in estado[0]["workers"]) == [3, 3],
                  ", ".join(f"{w['nombre']}: {w['hilos']} hilos" for w in estado[0]["workers"]) if estado else "")
        c.cerrar()

        # 9. Varios clientes a la vez: cada uno recibe solo sus resultados
        conexiones = [Conexion() for _ in range(3)]
        for n, cx in enumerate(conexiones):
            for i in range(4):
                cx.tarea("invertir", {"texto": f"c{n}t{i}"}, f"{n}-{i}")
        ok = all(cx.esperar(contar(4)) for cx in conexiones)
        propios = all(all(m["ref"].startswith(f"{n}-") and m["resultado"] == f"c{n}t{m['ref'].split('-')[1]}"[::-1]
                          for m in cx.de_tipo("resultado")) for n, cx in enumerate(conexiones))
        comprobar("3 clientes simultáneos reciben solo sus propios resultados", ok and propios)
        for cx in conexiones:
            cx.cerrar()

        # 10. Tolerancia a fallos: se cae un worker con tareas en proceso
        c = Conexion()
        for i in range(6):
            c.tarea("invertir", {"texto": f"falla{i}", "demora": 2}, f"f{i}")
        time.sleep(0.8)
        detener(w1)
        ok = c.esperar(contar(6), limite=30)
        quienes = {m["worker"] for m in c.de_tipo("resultado")}
        comprobar("Con un worker caído todas las tareas terminan", ok, f"workers que respondieron: {sorted(quienes)}")
        comprobar("Las tareas del worker caído las completa otro worker", quienes == {"worker-2"}, str(sorted(quienes)))
        c.cerrar()

        # 11. Persistencia en la base de datos
        time.sleep(0.5)
        bd = sqlite3.connect(os.path.join(carpeta, "tareas.db"))
        filas = bd.execute("SELECT estado, COUNT(*) FROM tareas GROUP BY estado").fetchall()
        bd.close()
        estados = dict(filas)
        comprobar("Todas las tareas quedaron registradas como completadas o fallidas",
                  set(estados) <= {"completada", "fallida"} and estados.get("fallida") == 1, str(estados))
    finally:
        for proceso in procesos:
            if proceso.poll() is None:
                detener(proceso)
            proceso.log.close()
        for origen, destino in (("servidor.log", "log_servidor.txt"), ("worker-1.log", "log_worker-1.txt"),
                                ("worker-2.log", "log_worker-2.txt")):
            if os.path.exists(os.path.join(carpeta, origen)):
                shutil.copy(os.path.join(carpeta, origen), os.path.join(RAIZ, "pruebas", destino))
        shutil.rmtree(carpeta, ignore_errors=True)

    print(f"\n{sum(resultados)} de {len(resultados)} pruebas correctas")
    return 0 if all(resultados) else 1


if __name__ == "__main__":
    sys.exit(main())
