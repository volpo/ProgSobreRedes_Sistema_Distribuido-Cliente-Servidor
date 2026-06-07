import json
import socket


def enviar_json(conn: socket.socket, data: dict) -> None:
    mensaje = (json.dumps(data, ensure_ascii=False) + "\n").encode("utf-8")
    conn.sendall(mensaje)


def recibir_linea(conn: socket.socket) -> bytes:
    buffer = b""
    while b"\n" not in buffer:
        chunk = conn.recv(4096)
        if not chunk:
            raise ConnectionError("Conexión cerrada antes de completar el mensaje")
        buffer += chunk
    return buffer.split(b"\n", 1)[0]


def recibir_json(conn: socket.socket) -> dict:
    return json.loads(recibir_linea(conn).decode("utf-8"))
