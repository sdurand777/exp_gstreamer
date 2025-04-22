
#!/usr/bin/env python3
import sys
import binascii
import gi

# Importez ici votre module protobuf généré :
# from camera_pb2 import ImageInfo    # ou info_pb2.StreamInfo
import info_pb2
try:
    from info_pb2 import StreamInfo
    HAS_PROTOBUF = True
except ImportError:
    HAS_PROTOBUF = False

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

TCP_HOST = "127.0.0.1"
TCP_PORT = 7000

def parse_klv(buffer):
    """Récupère la payload KLV et la parse en protobuf si possible."""
    success, info = buffer.map(Gst.MapFlags.READ)
    if not success:
        return None
    data = info.data
    buffer.unmap(info)

    if len(data) < 20:
        return f"[KLV trop court] {len(data)} bytes"

    key = binascii.hexlify(data[:16]).decode('ascii')
    length = int.from_bytes(data[16:20], 'big')
    value = data[20:20+length]

    if HAS_PROTOBUF:
        try:
            msg = info_pb2.StreamInfo()  # ou StreamInfo()
            msg.ParseFromString(value)
            return msg
        except Exception as e:
            return f"[ProtoError] {e}"
    else:
        prefix = binascii.hexlify(value[:32]).decode('ascii')
        return f"[KLV] key={key} len={length} value0-32={prefix}…"

def on_new_klv_sample(sink):
    sample = sink.emit("pull-sample")
    buf = sample.get_buffer()
    pts = buf.pts
    secs = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1
    klv = parse_klv(buf)
    print(f"KLV @ {secs:.3f}s → {klv}")
    return Gst.FlowReturn.OK

def main():

    print("----- main ------")

    Gst.init(None)

    # Pipeline minimale : tcpclientsrc -> tsdemux -> meta/x-klv -> appsink
    pipeline = Gst.parse_launch(
        f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! queue ! tsdemux name=dmx "
        "dmx. ! queue ! meta/x-klv ! queue ! appsink name=klv_sink emit-signals=true drop=true"
    )
    sink = pipeline.get_by_name("klv_sink")
    sink.connect("new-sample", on_new_klv_sample)

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect("message", lambda b, m: loop.quit() if m.type in (Gst.MessageType.ERROR, Gst.MessageType.EOS) else None)

    pipeline.set_state(Gst.State.PLAYING)
    print(f"Client KLV connecté à tcp://{TCP_HOST}:{TCP_PORT}, attente des trames…")
    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    main()
