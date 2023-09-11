import logging
import asyncio
import subprocess
import argparse
from asyncua import Client
import revpimodio2
import time

rpi = revpimodio2.RevPiModIO(autorefresh=True)

logging.basicConfig(level=logging.INFO)
_logger = logging.getLogger("asyncua")

# Argument parsing
parser = argparse.ArgumentParser(description='OPC-UA client to monitor variable changes and trigger scripts.')

parser.add_argument('--endpoint', type=str, help='OPC-UA server endpoint', required=True)
parser.add_argument('--namespace-uri', type=str, default='http://devnetiot.com/opcua/', help='Namespace URI (default: http://devnetiot.com/opcua/)')
parser.add_argument('--variables', type=str, nargs='+', help='List of variable names to monitor', required=True)
parser.add_argument('--string-values', type=str, nargs='+', help='List of string values to trigger script execution', required=True)
parser.add_argument('--scripts', type=str, nargs='+', help='List of scripts corresponding to string values', required=True)

args = parser.parse_args()
OPCUA_ENDPOINT = args.endpoint
NAMESPACE_URI = args.namespace_uri  # This will use the default if not provided
OPCUA_VARS = args.variables
STRING_VALUES = args.string_values
SCRIPTS = args.scripts



class SubHandler(object):
    """
    Subscription Handler Class. To receive events from server for a subscription
    """

    def datachange_notification(self, node, val, data):
        print(f"New data change event on node {node}, with value {val}")
    
        # Check if the value matches any predefined string values
        if val in STRING_VALUES:
            index = STRING_VALUES.index(val)
            script_to_run = SCRIPTS[index]
        
            rpi.io.RevPiLED.value = int(SCRIPTS[index])
            # Execute the script
            #subprocess.run([script_to_run])
            print(f"Set LED value: {script_to_run}")
        else :
            rpi.io.RevPiLED.value = 0

    def event_notification(self, event):
        """
        Called for events
        """
        print("Python: New event", event)


async def main():
    client = Client(url=OPCUA_ENDPOINT)
    async with client:
        _logger.info(f"Connected to {OPCUA_ENDPOINT}")

        # Get the namespace index based on the URI
        idx = await client.get_namespace_index(NAMESPACE_URI)

        # Get object node
        obj_vplc = await client.nodes.root.get_child(["0:Objects", f"{idx}:vPLC2"])
        
        handler = SubHandler()
        # Create subscription
        #sub = await client.create_subscription(500, datachange_notification)
        sub = await client.create_subscription(500, handler)

        # Subscribe to variables
        for var_name in OPCUA_VARS:
            var_node = await obj_vplc.get_child([f"{idx}:{var_name}"])
            await sub.subscribe_data_change(var_node)
        
        while True:
            await asyncio.sleep(1)

if __name__ == "__main__":
    asyncio.run(main())
