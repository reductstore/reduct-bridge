import time
import random
import math
import paho.mqtt.client as mqtt
import factory_pb2


def gen_fft_bins(n=64):
    base_freq = random.uniform(30, 120)
    bins = []
    for i in range(n):
        freq = i * 10
        amplitude = random.uniform(0.001, 0.01)
        if abs(freq - base_freq) < 15:
            amplitude += random.uniform(0.5, 2.0)
        if abs(freq - base_freq * 2) < 10:
            amplitude += random.uniform(0.1, 0.5)
        bins.append(amplitude)
    return bins


client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2, client_id="factory-sim")
client.connect("mosquitto", 1883)

lines = ["line-1", "line-2", "line-3"]
machines = ["cnc-01", "cnc-02", "press-01", "conveyor-03"]
panels = ["mcc-panel-A", "mcc-panel-B", "dist-panel-1"]
axes = ["x", "y", "z"]

energy_accum = {p: random.uniform(1000, 5000) for p in panels}
tick = 0

while True:
    ts = int(time.time() * 1000)

    # Environment sensors on each production line (every tick)
    for line in lines:
        msg = factory_pb2.EnvironmentReading(
            device_id=f"env/{line}/sensor-01",
            line=line,
            temperature_c=22.0
            + random.gauss(0, 1.5)
            + (0.5 if line == "line-3" else 0),
            humidity_pct=45.0 + random.gauss(0, 3.0),
            pressure_hpa=1013.0 + random.gauss(0, 0.5),
            timestamp_ms=ts,
        )
        client.publish(f"factory/{line}/environment", msg.SerializeToString(), qos=0)

    # Vibration on machines (every 2 ticks)
    if tick % 2 == 0:
        for machine in machines:
            for axis in axes:
                fft = gen_fft_bins()
                rms = math.sqrt(sum(x * x for x in fft) / len(fft))
                peak = max(fft)
                msg = factory_pb2.VibrationSpectrum(
                    device_id=f"vib/{machine}/{axis}",
                    machine=machine,
                    axis=axis,
                    fft_bins=fft,
                    rms_velocity=rms,
                    peak_g=peak,
                    timestamp_ms=ts,
                )
                client.publish(
                    f"factory/machines/{machine}/vibration/{axis}",
                    msg.SerializeToString(),
                    qos=0,
                )

    # Power readings (every 5 ticks)
    if tick % 5 == 0:
        for panel in panels:
            voltage = 400.0 + random.gauss(0, 2.0)
            current = random.uniform(50, 200)
            power = voltage * current * 0.85
            energy_accum[panel] += power * 5.0 / 3600000.0
            msg = factory_pb2.PowerReading(
                device_id=f"power/{panel}/meter",
                panel=panel,
                voltage_v=voltage,
                current_a=current,
                power_w=power,
                energy_kwh=energy_accum[panel],
                frequency_hz=50.0 + random.gauss(0, 0.02),
                timestamp_ms=ts,
            )
            client.publish(
                f"factory/electrical/{panel}/power", msg.SerializeToString(), qos=0
            )

    tick += 1
    time.sleep(1)
