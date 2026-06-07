import argparse
import json
import logging
import socket
import threading
from queue import Queue

from protocolo import enviar_json, recibir_json

WORKERS = 4

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
logger = logging.getLogger(__name__)

task_queue: Queue = Queue()


def procesar_tarea(payload: dict) -> dict:
    operacion = payload.get("operacion")
    datos = payload.get("datos")

    if operacion == "sumar":
        if not isinstance(datos, list) or not all(
            isinstance(n, (int, float)) for n in datos
        ):
            raise ValueError('La operación "sumar" requiere una lista numérica en "datos"')
        return {"resultado": sum(datos)}

    if operacion == "invertir":
        if not isinstance(datos, str):
            raise ValueError('La operación "invertir" requiere un texto en "datos"')
        return {"resultado": datos[::-1]}

    if operacion == "mayusculas":
        if not isinstance(datos, str):
            raise ValueError('La operación "mayusculas" requiere un texto en "datos"')
        return {"resultado": datos.upper()}

    if operacion == "ping":
        return {"resultado": "pong"}

    raise ValueError(f'Operación no soportada: "{operacion}"')


def worker_loop(worker_id: int, servidor_id: int) -> None:
    while True:
        conn, addr, payload = task_queue.get()
        try:
            logger.info(
                "Servidor %s - hilo %s procesando tarea de %s",
                servidor_id,
                worker_id,
                addr,
            )
            resultado = procesar_tarea(payload)
            respuesta = {
                "estado": "ok",
                "servidor": servidor_id,
                "worker": worker_id,
                **resultado,
            }
        except ValueError as exc:
            respuesta = {
                "estado": "error",
                "servidor": servidor_id,
                "worker": worker_id,
                "error": str(exc),
            }
        except Exception as exc:
            respuesta = {
                "estado": "error",
                "servidor": servidor_id,
                "worker": worker_id,
                "error": f"Error interno: {exc}",
            }

        try:
            enviar_json(conn, respuesta)
        except OSError:
            logger.warning("No se pudo responder al cliente %s", addr)
        finally:
            conn.close()
            task_queue.task_done()


def manejar_cliente(conn: socket.socket, addr) -> None:
    try:
        payload = recibir_json(conn)
        logger.info("Tarea recibida de %s: %s", addr, payload)
        task_queue.put((conn, addr, payload))
    except (json.JSONDecodeError, UnicodeDecodeError):
        enviar_json(conn, {"estado": "error", "error": "JSON inválido"})
        conn.close()
    except ConnectionError as exc:
        logger.warning("Conexión incompleta desde %s: %s", addr, exc)
        conn.close()
    except OSError as exc:
        logger.error("Error de socket con %s: %s", addr, exc)
        conn.close()


def iniciar_pool(servidor_id: int) -> None:
    for worker_id in range(1, WORKERS + 1):
        threading.Thread(
            target=worker_loop,
            args=(worker_id, servidor_id),
            daemon=True,
            name=f"servidor-{servidor_id}-worker-{worker_id}",
        ).start()


def main() -> None:
    parser = argparse.ArgumentParser(description="Servidor worker con pool de hilos")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, required=True)
    parser.add_argument("--id", type=int, required=True, help="Identificador del servidor worker")
    args = parser.parse_args()

    iniciar_pool(args.id)

    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as server:
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind((args.host, args.port))
        server.listen(10)
        logger.info(
            "Servidor worker %s escuchando en %s:%s (%s hilos)",
            args.id,
            args.host,
            args.port,
            WORKERS,
        )

        while True:
            conn, addr = server.accept()
            logger.info("Servidor worker %s: cliente conectado %s", args.id, addr)
            threading.Thread(
                target=manejar_cliente,
                args=(conn, addr),
                daemon=True,
            ).start()


if __name__ == "__main__":
    main()
