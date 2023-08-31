import logging
import asyncio
from asyncua import ua, Server
import paho.mqtt.client as mqtt
import argparse
import queue as sync_queue  # Python's built-in synchronous queue

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger('asyncua')

parser = argparse.ArgumentParser(description='MQTT to OPC UA Bridge')
parser.add_argument('--mqtt_server', type=str, default='192.168.40.93', help='MQTT server address')
parser.add_argument('--mqtt_port', type=int, default=1883, help='MQTT port')
parser.add_argument('--opcua_endpoint', type=str, default='opc.tcp://0.0.0.0:4844/opcua/', help='OPC UA endpoint')
parser.add_argument('--mqtt_topics', nargs='+', type=str, help='List of MQTT topics')
parser.add_argument('--opcua_vars', nargs='+', type=str, help='List of OPC UA variables')
args = parser.parse_args()

if len(args.mqtt_topics) != len(args.opcua_vars):
    print("Error: The number of MQTT topics must match the number of OPC UA variables.")
    exit(1)

MQTT_BROKER = args.mqtt_server
MQTT_PORT = args.mqtt_port
OPCUA_ENDPOINT = args.opcua_endpoint
MQTT_TOPICS = args.mqtt_topics
OPCUA_VARS = args.opcua_vars

queue = asyncio.Queue()
sync_q = sync_queue.Queue()

def on_message(client, userdata, message):
    print("Received MQTT message.")
    topic = message.topic
    payload = message.payload.decode("utf-8")
    sync_q.put((topic, payload))
    print(f"Pushed to sync queue: {topic}, {payload}")

async def process_queue(var_mapping):
    if not queue.qsize() == 0:
        topic, payload = await queue.get()
        _logger.info(f"### process_queue: {topic}, {payload}")
        if topic in var_mapping:
            await var_mapping[topic].write_value(payload)
            _logger.info(f"Written to {topic}")

async def main():
    server = Server()
    await server.init()
    server.set_endpoint(OPCUA_ENDPOINT)
    server.set_server_name("OPC-UA Object Detection Server")

    uri = 'http://devnetiot.com/opcua/'
    idx = await server.register_namespace(uri)

    obj_vplc = await server.nodes.objects.add_object(idx, 'vPLC2')
    
    var_mapping = {}
    #for opcua_var in OPCUA_VARS:
    for i, opcua_var in enumerate(OPCUA_VARS):
        var_mapping[MQTT_TOPICS[i]] = await obj_vplc.add_variable(idx, opcua_var, 0)

    mqtt_client = mqtt.Client()
    mqtt_client.on_message = on_message
    mqtt_client.connect(MQTT_BROKER, MQTT_PORT, 60)

    for topic in MQTT_TOPICS:
        _logger.info(f"### mqtt subscribe: {topic}")
        mqtt_client.subscribe(topic)

    print("Starting MQTT client...")
    mqtt_client.loop_start()
    print("MQTT client started.")
    _logger.info('Starting server!')

    async with server:
        while True:
            print("Running main loop...")
            if not sync_q.empty():
                topic, payload = sync_q.get()
                await queue.put((topic, payload))
                print(f"Moved to async queue: {topic}, {payload}")
            await process_queue(var_mapping)
            await asyncio.sleep(1)

if __name__ == '__main__':
    print("Main event loop set.")
    asyncio.run(main())

# python3 opc-ua-sensor-simulator/mqtt6.py --mqtt_topics "video_analysis/message" "$$SYS/broker/uptime" "opcua/ns_2_i_2" --opcua_vars object uptime temp --mqtt_server 192.168.40.93 --mqtt_port 1883 --opcua_endpoint opc.tcp://0.0.0.0:4844/opcua/    
