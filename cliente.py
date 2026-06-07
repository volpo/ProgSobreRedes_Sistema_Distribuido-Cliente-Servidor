import argparse
import json
import socket
import sys


HOST_DEFAULT = "127.0.0.1"
PORT_DEFAULT = 9000


def enviar_json(conn: socket.socket, data: dict) -> None:
    mensaje = (json.dumps(data, ensure_ascii=False) + "\n").encode("utf-8")
    conn.sendall(mensaje)


def recibir_json(conn: socket.socket) -> dict:
    buffer = b""
    while b"\n" not in buffer:
        chunk = conn.recv(4096)
        if not chunk:
            raise ConnectionError("Conexión cerrada antes de completar el mensaje")
        buffer += chunk
    return json.loads(buffer.split(b"\n", 1)[0].decode("utf-8"))


def enviar_tarea(host: str, port: int, operacion: str, datos) -> dict:
    payload = {"operacion": operacion, "datos": datos}

    with socket.create_connection((host, port), timeout=10) as conn:
        enviar_json(conn, payload)
        return recibir_json(conn)


def parse_datos(valor: str):
    try:
        return json.loads(valor)
    except json.JSONDecodeError:
        return valor


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Cliente que envía tareas al balanceador por socket"
    )
    parser.add_argument("--host", default=HOST_DEFAULT, help="Host del balanceador")
    parser.add_argument("--port", type=int, default=PORT_DEFAULT, help="Puerto del balanceador")
    parser.add_argument(
        "--operacion",
        required=True,
        choices=["sumar", "invertir", "mayusculas", "ping"],
        help="Operación a ejecutar",
    )
    parser.add_argument(
        "--datos",
        default="",
        help='Datos de la tarea. Ej: "hola", "[1,2,3]" o vacío para ping',
    )
    args = parser.parse_args()

    if args.operacion == "ping":
        datos = None
    else:
        if not args.datos:
            print("Debe indicar --datos para esta operación", file=sys.stderr)
            sys.exit(1)
        datos = parse_datos(args.datos)

    try:
        respuesta = enviar_tarea(args.host, args.port, args.operacion, datos)
    except (ConnectionRefusedError, TimeoutError, OSError) as exc:
        print(f"No se pudo conectar al balanceador: {exc}", file=sys.stderr)
        sys.exit(1)

    print(json.dumps(respuesta, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
