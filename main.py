import asyncio
import json
import struct
import socket
import requests
import websockets
import os

TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = int(os.environ["GUILD_ID"])
CHANNEL_ID = int(os.environ["CHANNEL_ID"])
STATUS = os.environ.get("STATUS", "online")
SELF_MUTE = os.environ.get("SELF_MUTE", "true").strip().lower() == "true"
SELF_DEAF = os.environ.get("SELF_DEAF", "true").strip().lower() == "true"

API = "https://discord.com/api/v10"

res = requests.get(f"{API}/users/@me", headers={"Authorization": TOKEN})
if res.status_code != 200:
    print("Invalid token!")
    exit()
user = res.json()
print(f"Logged in as {user['username']} ({user['id']})!")


class VoiceConnection:
    """
    Completes the voice gateway + UDP handshake that Discord requires
    before it treats a voice session as "real". Without this, the main
    gateway op4 VOICE_STATE_UPDATE gets silently reverted by Discord
    after a short timeout, which is why self_mute/self_deaf looked
    correct in the logs but never showed up as crossed-out icons in
    the channel.
    """

    def __init__(self, endpoint, token, session_id, server_id, user_id):
        self.endpoint = endpoint
        self.token = token
        self.session_id = session_id
        self.server_id = server_id
        self.user_id = user_id
        self.ws = None
        self.ssrc = None
        self.udp_ip = None
        self.udp_port = None
        self.mode = None
        self.external_ip = None
        self.external_port = None
        self.secret_key = None
        self.sock = None
        self._heartbeat_task = None
        self._keepalive_task = None
        self._event_task = None

    async def connect(self):
        uri = f"wss://{self.endpoint}?v=4"
        self.ws = await websockets.connect(uri, max_size=10 * 1024 * 1024)

        await self.ws.send(json.dumps({
            "op": 0,  # IDENTIFY
            "d": {
                "server_id": str(self.server_id),
                "user_id": str(self.user_id),
                "session_id": self.session_id,
                "token": self.token,
            }
        }))

        hello = json.loads(await self.ws.recv())
        if hello.get("op") != 8:
            print("Unexpected voice op waiting for HELLO:", hello)
            return False

        heartbeat_interval = hello["d"]["heartbeat_interval"]
        self._heartbeat_task = asyncio.create_task(self._heartbeat_loop(heartbeat_interval))

        ready = json.loads(await self.ws.recv())
        if ready.get("op") != 2:  # READY
            print("Unexpected voice op waiting for READY:", ready)
            return False

        d = ready["d"]
        self.ssrc = d["ssrc"]
        self.udp_ip = d["ip"]
        self.udp_port = d["port"]
        modes = d["modes"]
        self.mode = "xsalsa20_poly1305" if "xsalsa20_poly1305" in modes else modes[0]

        if not await self._udp_discovery():
            return False

        await self.ws.send(json.dumps({
            "op": 1,  # SELECT_PROTOCOL
            "d": {
                "protocol": "udp",
                "data": {
                    "address": self.external_ip,
                    "port": self.external_port,
                    "mode": self.mode,
                }
            }
        }))

        session_desc = json.loads(await self.ws.recv())
        if session_desc.get("op") != 4:  # SESSION_DESCRIPTION
            print("Unexpected op waiting for SESSION_DESCRIPTION:", session_desc)
            return False

        self.secret_key = session_desc["d"]["secret_key"]
        print("Voice session fully established (SESSION_DESCRIPTION received).")

        self._keepalive_task = asyncio.create_task(self._udp_keepalive())
        self._event_task = asyncio.create_task(self._voice_event_loop())
        return True

    async def _heartbeat_loop(self, interval_ms):
        try:
            while True:
                await asyncio.sleep(interval_ms / 1000)
                await self.ws.send(json.dumps({
                    "op": 3,
                    "d": int(asyncio.get_event_loop().time() * 1000)
                }))
        except (asyncio.CancelledError, websockets.exceptions.ConnectionClosed):
            pass

    async def _udp_discovery(self):
        loop = asyncio.get_event_loop()
        try:
            self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            self.sock.setblocking(False)
            self.sock.connect((self.udp_ip, self.udp_port))

            packet = bytearray(74)
            struct.pack_into(">H", packet, 0, 1)    # request type
            struct.pack_into(">H", packet, 2, 70)   # payload length
            struct.pack_into(">I", packet, 4, self.ssrc)
            await loop.sock_sendall(self.sock, packet)

            data = await asyncio.wait_for(loop.sock_recv(self.sock, 74), timeout=10)
            self.external_ip = data[8:72].split(b"\x00", 1)[0].decode()
            self.external_port = struct.unpack_from(">H", data, 72)[0]
            print(f"UDP discovery resolved external address: {self.external_ip}:{self.external_port}")
            return True
        except (OSError, asyncio.TimeoutError) as e:
            print("UDP discovery failed:", e)
            return False

    async def _udp_keepalive(self):
        """Discord drops idle voice UDP sessions after a few minutes of
        silence. This sends a periodic keepalive so the session (and the
        mute/deaf state tied to it) stays alive without real audio."""
        loop = asyncio.get_event_loop()
        try:
            while True:
                await asyncio.sleep(5)
                try:
                    await loop.sock_sendall(self.sock, struct.pack(">I", self.ssrc))
                except OSError as e:
                    print("UDP keepalive error:", e)
        except asyncio.CancelledError:
            pass

    async def _voice_event_loop(self):
        try:
            async for msg in self.ws:
                json.loads(msg)  # speaking updates / heartbeat acks, nothing to act on
        except websockets.exceptions.ConnectionClosed as e:
            print("Voice gateway closed:", e)

    async def close(self):
        for task in (self._heartbeat_task, self._keepalive_task, self._event_task):
            if task:
                task.cancel()
        if self.sock:
            self.sock.close()
        if self.ws:
            await self.ws.close()


async def heartbeat(ws, interval):
    while True:
        await asyncio.sleep(interval / 1000)
        await ws.send(json.dumps({"op": 1, "d": None}))


async def main():
    uri = "wss://gateway.discord.gg/?v=10&encoding=json"
    async with websockets.connect(uri, max_size=10 * 1024 * 1024) as ws:
        hello = json.loads(await ws.recv())
        heartbeat_interval = hello["d"]["heartbeat_interval"]
        asyncio.create_task(heartbeat(ws, heartbeat_interval))

        await ws.send(json.dumps({
            "op": 2,
            "d": {
                "token": TOKEN,
                "properties": {
                    "$os": "windows",
                    "$browser": "chrome",
                    "$device": "pc"
                },
                "presence": {
                    "status": STATUS,
                    "afk": False
                }
            }
        }))

        while True:
            event = json.loads(await ws.recv())
            if event.get("t") == "READY":
                break

        print("MUTE VALUE:", SELF_MUTE, type(SELF_MUTE))
        print("DEAF VALUE:", SELF_DEAF, type(SELF_DEAF))

        await ws.send(json.dumps({
            "op": 4,
            "d": {
                "guild_id": GUILD_ID,
                "channel_id": CHANNEL_ID,
                "self_mute": SELF_MUTE,
                "self_deaf": SELF_DEAF
            }
        }))
        print("Voice state request sent!")

        session_id = None
        voice_conn = None

        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get("t") == "VOICE_STATE_UPDATE":
                    voice = data.get("d", {})
                    if voice.get("user_id") == user["id"]:
                        session_id = voice.get("session_id")
                        print("DISCORD VOICE STATE:")
                        print("SELF MUTE:", voice.get("self_mute"))
                        print("SELF DEAF:", voice.get("self_deaf"))
                        print("SERVER MUTE:", voice.get("mute"))
                        print("SERVER DEAF:", voice.get("deaf"))
                        print("CHANNEL:", voice.get("channel_id"))

                elif data.get("t") == "VOICE_SERVER_UPDATE":
                    d = data["d"]
                    voice_token = d["token"]
                    voice_endpoint = d["endpoint"]
                    print("VOICE_SERVER_UPDATE received, endpoint:", voice_endpoint)

                    if session_id and voice_endpoint:
                        if voice_conn:
                            await voice_conn.close()
                        voice_conn = VoiceConnection(
                            endpoint=voice_endpoint,
                            token=voice_token,
                            session_id=session_id,
                            server_id=GUILD_ID,
                            user_id=user["id"],
                        )
                        ok = await voice_conn.connect()
                        if ok:
                            print("Voice connection fully handshaked. Mute/deafen icons should now persist.")
                        else:
                            print("Voice handshake failed, will retry on next VOICE_SERVER_UPDATE.")

            except Exception as e:
                print("Voice error:", e)
                break


async def run():
    while True:
        try:
            await main()
        except Exception as e:
            print("Error: ", e)
            await asyncio.sleep(5)

asyncio.run(run())
