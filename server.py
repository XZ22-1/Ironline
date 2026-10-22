"""Two-player input relay. Run one process/instance; TLS terminates at the hosting proxy."""
import asyncio
import json
import os
import secrets
import time
from http import HTTPStatus
from websockets.asyncio.server import serve
from websockets.exceptions import ConnectionClosed

VERSION = "ironline-19"
ROOMS = {}
CONNECTIONS = set()
MAX_ROOMS = 200

async def send(ws, **message):
    await asyncio.wait_for(ws.send(json.dumps(message, separators=(",", ":"))), 3)

class Room:
    def __init__(self, code, ws, weapon):
        self.code = code
        self.peers = [ws, None]
        self.weapons = [weapon, 0]
        self.ready = [False, False]
        self.active = False
        self.match = 0
        self.tick = 0
        self.queues = [{}, {}]
        self.expected = [0, 0]
        self.checks = {}
        self.ends = {}
        self.last_progress = time.monotonic()
        self.created = time.monotonic()
        self.lock = asyncio.Lock()

    async def broadcast(self, **message):
        await asyncio.gather(*(send(p, **message) for p in self.peers if p))

    async def lobby(self):
        for slot, peer in enumerate(self.peers):
            if peer:
                await send(peer, type="room", code=self.code, slot=slot,
                           w0=self.weapons[0], w1=self.weapons[1],
                           r0=self.ready[0], r1=self.ready[1], joined=self.peers[1] is not None)

def weapon(value):
    if type(value) is not int or not 0 <= value < 6:
        raise ValueError("武器选择无效")
    return value

def command(value):
    if not isinstance(value, dict):
        raise ValueError("操作数据无效")
    result = {}
    for field in ("move", "vertical"):
        v = value.get(field, 0)
        if type(v) not in (float, int) or v not in (-1, 0, 1):
            raise ValueError("移动数据无效")
        result[field] = v
    for field in ("basic", "skill1", "skill2", "ultimate", "escape", "dodge"):
        v = value.get(field, False)
        if type(v) is not bool:
            raise ValueError("操作数据无效")
        result[field] = v
    return result

async def handle_room(room, slot, message):
    kind = message.get("type")
    if kind == "choose" and not room.active:
        room.weapons[slot] = weapon(message.get("weapon"))
        room.ready[slot] = False
        await room.lobby()
    elif kind == "ready" and not room.active:
        room.ready[slot] = message.get("ready") is True
        if all(room.peers) and all(room.ready):
            room.active = True
            room.match += 1
            room.tick = 0
            room.expected = [0, 0]
            room.queues = [{}, {}]
            room.checks.clear()
            room.ends.clear()
            room.last_progress = time.monotonic()
            await room.broadcast(type="start", match=room.match, w0=room.weapons[0], w1=room.weapons[1])
        else:
            await room.lobby()
    elif kind in ("input", "check", "end"):
        if not room.active or message.get("match") != room.match:
            return  # Late packets from a completed match cannot enter the next one.
        tick = message.get("tick")
        if type(tick) is not int:
            raise ValueError("帧编号无效")
        if kind == "input":
            if tick != room.expected[slot] or tick > room.tick + 30:
                raise ValueError("操作顺序无效")
            room.queues[slot][tick] = command(message.get("command"))
            room.expected[slot] += 1
            while all(room.tick in q for q in room.queues):
                a, b = (q.pop(room.tick) for q in room.queues)
                await room.broadcast(type="frame", match=room.match, tick=room.tick, a=a, b=b)
                room.tick += 1
                room.last_progress = time.monotonic()
        else:
            digest = message.get("hash")
            if not isinstance(digest, str) or len(digest) != 64 or tick < 0 or tick > room.tick:
                raise ValueError("同步校验无效")
            if kind == "check":
                if tick % 60 or tick < room.tick - 180:
                    raise ValueError("同步校验已过期")
                entries = room.checks.setdefault(tick, {})
                entries[slot] = digest
                if len(entries) == 2:
                    if entries[0] != entries[1]:
                        raise ValueError("双方画面状态不一致，请重新创建房间")
                    del room.checks[tick]
                if len(room.checks) > 8:
                    raise ValueError("对方未完成同步校验")
            else:
                winner = message.get("winner")
                if winner not in (-1, 0, 1):
                    raise ValueError("结算数据无效")
                room.ends[slot] = (tick, digest, winner)
                if len(room.ends) == 2:
                    if room.ends[0] != room.ends[1]:
                        raise ValueError("双方结算不一致，请重新创建房间")
                    room.active = False
                    room.ready = [False, False]
                    await room.broadcast(type="result", winner=winner)

async def handler(ws):
    if len(CONNECTIONS) >= 450:
        await ws.close(1013, "Service busy")
        return
    CONNECTIONS.add(ws)
    room = None
    try:
        message = json.loads(await asyncio.wait_for(ws.recv(), 10))
        if not isinstance(message, dict) or message.get("version") != VERSION:
            raise ValueError("游戏版本不同，请双方使用 v19")
        kind = message.get("type")
        if kind == "create":
            if len(ROOMS) >= MAX_ROOMS:
                raise ValueError("房间已满，请稍后重试")
            code = "".join(secrets.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(6))
            while code in ROOMS:
                code = "".join(secrets.choice("ABCDEFGHJKLMNPQRSTUVWXYZ23456789") for _ in range(6))
            room = Room(code, ws, weapon(message.get("weapon")))
            ROOMS[code] = room
            slot = 0
            await room.lobby()
        elif kind == "join":
            code = str(message.get("code", "")).upper().strip()
            target = ROOMS.get(code)
            if target is None:
                raise ValueError("房间不存在或已关闭，请检查房间码")
            async with target.lock:
                if target.peers[1] is not None:
                    raise ValueError("房间已有两位玩家")
                selected = weapon(message.get("weapon"))
                room = target
                slot = 1
                room.peers[1] = ws
                room.weapons[1] = selected
                await room.lobby()
        else:
            raise ValueError("请先创建或加入房间")
        window, count = time.monotonic(), 0
        async for raw in ws:
            now = time.monotonic()
            if now - window >= 1:
                window, count = now, 0
            count += 1
            if count > 180:
                raise ValueError("发送过于频繁")
            message = json.loads(raw)
            if not isinstance(message, dict):
                raise ValueError("消息无效")
            if message.get("type") == "ping":
                await send(ws, type="pong", stamp=message.get("stamp", 0))
                continue
            async with room.lock:
                await handle_room(room, slot, message)
    except (ValueError, TypeError) as exc:
        try:
            await send(ws, type="error", message=str(exc))
        except (ConnectionClosed, TimeoutError):
            pass
    except (ConnectionClosed, TimeoutError):
        pass
    finally:
        CONNECTIONS.discard(ws)
        if room and ROOMS.get(room.code) is room:
            del ROOMS[room.code]
            for peer in room.peers:
                if peer and peer is not ws:
                    try:
                        await send(peer, type="error", message="对方已离开，房间已关闭")
                        await peer.close()
                    except (ConnectionClosed, TimeoutError):
                        pass

async def watchdog():
    while True:
        await asyncio.sleep(2)
        now = time.monotonic()
        for room in list(ROOMS.values()):
            if (room.active and now - room.last_progress > 15) or now - room.created > 7200:
                for peer in room.peers:
                    if peer:
                        await peer.close(1000, "Room timed out")

def health(connection, request):
    if request.path == "/health":
        return connection.respond(HTTPStatus.OK, "IRONLINE relay ready\n")

async def main():
    host, port = os.getenv("HOST", "127.0.0.1"), int(os.getenv("PORT", "8765"))
    async with serve(handler, host, port, max_size=4096, max_queue=32,
                     compression=None, ping_interval=10, ping_timeout=10,
                     process_request=health):
        print(f"IRONLINE_RELAY_READY {host}:{port}", flush=True)
        await watchdog()

if __name__ == "__main__":
    asyncio.run(main())


