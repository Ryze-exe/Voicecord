import asyncio
import json
import requests
import websockets
import os

TOKEN = os.environ["DISCORD_TOKEN"]
GUILD_ID = int(os.environ["GUILD_ID"])
CHANNEL_ID = int(os.environ["CHANNEL_ID"])


STATUS = os.environ.get("STATUS", "online")
SELF_MUTE = os.environ.get("SELF_MUTE", "true").lower() == "true"
SELF_DEAF = os.environ.get("SELF_DEAF", "true").lower() == "true"

API = "https://discord.com/api/v10"

res = requests.get(f"{API}/users/@me", headers={"Authorization": TOKEN})
if res.status_code != 200:
    print("Invalid token!")
    exit()

user = res.json()
print(f"Logged in as {user['username']} ({user['id']})!")

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

        while True:
            try:
                msg = await ws.recv()
                data = json.loads(msg)

                if data.get("t") == "VOICE_STATE_UPDATE":
                    voice = data.get("d", {})

                    if voice.get("user_id") == user["id"]:
                        print("DISCORD VOICE STATE:")
                        print("SELF MUTE:", voice.get("self_mute"))
                        print("SELF DEAF:", voice.get("self_deaf"))
                        print("SERVER MUTE:", voice.get("mute"))
                        print("SERVER DEAF:", voice.get("deaf"))
                        print("CHANNEL:", voice.get("channel_id"))

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
