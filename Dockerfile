FROM python:bookworm

RUN git clone https://github.com/SebSixSq/opc-ua-sensor-simulator.git
RUN pip install -r opc-ua-sensor-simulator/requirements.txt
RUN unzip opc-ua-sensor-simulator/sensor.csv.zip
ENTRYPOINT python "opc-ua-sensor-simulator/opc-ua-server.py"

EXPOSE 4840/udp
EXPOSE 4840/tcp
