import asyncio
import json
import unittest
from websockets.asyncio.client import connect
from websockets.asyncio.server import serve
import server

class RelayTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self):
        server.ROOMS.clear()
        self.service = await serve(server.handler, "127.0.0.1", 0)
        self.url = "ws://127.0.0.1:" + str(self.service.sockets[0].getsockname()[1])
        self.clients = []

    async def asyncTearDown(self):
        for client in self.clients:
            await client.close()
        self.service.close()
        await self.service.wait_closed()

    async def client(self, **message):
        c = await connect(self.url, proxy=None)
        self.clients.append(c)
        await self.send(c, version=server.VERSION, **message)
        return c

    async def send(self, c, **message):
        await c.send(json.dumps(message))

    async def recv(self, c, kind):
        while True:
            m = json.loads(await asyncio.wait_for(c.recv(), 3))
            if m["type"] == kind:
                return m

    async def pair(self):
        a = await self.client(type="create", weapon=3)
        code = (await self.recv(a, "room"))["code"]
        b = await self.client(type="join", code=code, weapon=1)
        self.assertEqual((await self.recv(b, "room"))["slot"], 1)
        await self.recv(a, "room")
        await self.send(a, type="ready", ready=True)
        await self.recv(a, "room"); await self.recv(b, "room")
        await self.send(b, type="ready", ready=True)
        x, y = await self.recv(a, "start"), await self.recv(b, "start")
        self.assertEqual(x, y)
        self.assertEqual((x["w0"], x["w1"]), (3, 1))
        return a, b, x["match"], code

    async def test_pair_frames_and_matching_result_rematch(self):
        a, b, match, _ = await self.pair()
        for tick in range(3):
            await self.send(b, type="input", match=match, tick=tick, command={"move": -1, "skill1": True})
            await self.send(a, type="input", match=match, tick=tick, command={"move": 1})
            x, y = await self.recv(a, "frame"), await self.recv(b, "frame")
            self.assertEqual(x, y)
            self.assertEqual(x["a"]["move"], 1)
            self.assertTrue(x["b"]["skill1"])
        for c in (a, b):
            await self.send(c, type="end", match=match, tick=3, hash="a"*64, winner=1)
        self.assertEqual((await self.recv(a, "result"))["winner"], 1)
        await self.recv(b, "result")
        await self.send(a, type="ready", ready=True)
        await self.recv(a, "room"); await self.recv(b, "room")
        await self.send(b, type="ready", ready=True)
        self.assertEqual((await self.recv(a, "start"))["match"], match+1)
        await self.recv(b, "start")
        # A late old-round input must not advance the rematch.
        await self.send(a, type="input", match=match, tick=3, command={})
        for c in (a, b):
            await self.send(c, type="input", match=match+1, tick=0, command={})
        self.assertEqual((await self.recv(a, "frame"))["tick"], 0)

    async def test_room_full_does_not_close_existing_room(self):
        a, b, match, code = await self.pair()
        c = await self.client(type="join", code=code, weapon=0)
        self.assertIn("两位", (await self.recv(c, "error"))["message"])
        self.assertIn(code, server.ROOMS)

    async def test_missing_room(self):
        c = await self.client(type="join", code="ABCDEF", weapon=0)
        self.assertIn("不存在", (await self.recv(c, "error"))["message"])

    async def test_version_mismatch(self):
        c = await connect(self.url, proxy=None); self.clients.append(c)
        await self.send(c, type="create", version="old", weapon=0)
        self.assertIn("版本", (await self.recv(c, "error"))["message"])

    async def test_out_of_order_rejected(self):
        a, b, match, _ = await self.pair()
        await self.send(a, type="input", match=match, tick=2, command={})
        self.assertIn("顺序", (await self.recv(a, "error"))["message"])
        await self.recv(b, "error")

    async def test_invalid_movement_rejected(self):
        a, b, match, _ = await self.pair()
        await self.send(a, type="input", match=match, tick=0, command={"move": 100})
        self.assertIn("移动", (await self.recv(a, "error"))["message"])

    async def test_different_results_abort(self):
        a, b, match, _ = await self.pair()
        await self.send(a, type="end", match=match, tick=0, hash="a"*64, winner=0)
        await self.send(b, type="end", match=match, tick=0, hash="b"*64, winner=1)
        self.assertIn("不一致", (await self.recv(b, "error"))["message"])

    async def test_disconnect_closes_room(self):
        a, b, _, code = await self.pair()
        await b.close()
        self.assertIn("离开", (await self.recv(a, "error"))["message"])
        self.assertNotIn(code, server.ROOMS)

    async def test_change_weapon_cancels_ready(self):
        a = await self.client(type="create", weapon=0)
        await self.recv(a, "room")
        await self.send(a, type="ready", ready=True)
        self.assertTrue((await self.recv(a, "room"))["r0"])
        await self.send(a, type="choose", weapon=3)
        m = await self.recv(a, "room")
        self.assertFalse(m["r0"]); self.assertEqual(m["w0"], 3)

    async def test_delayed_peer_inputs_are_identical(self):
        a, b, match, _ = await self.pair()
        for tick in range(8):
            await self.send(a, type="input", match=match, tick=tick, command={"dodge": tick==2})
        await asyncio.sleep(.15)
        for tick in range(8):
            await self.send(b, type="input", match=match, tick=tick, command={"escape": tick==5})
            x, y = await self.recv(a, "frame"), await self.recv(b, "frame")
            self.assertEqual(x, y); self.assertEqual(x["tick"], tick)

if __name__ == "__main__":
    unittest.main()
