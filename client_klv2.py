
#!/usr/bin/env python3
import sys
import struct
import gi

# votre protobuf
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

TCP_HOST = "127.0.0.1"
TCP_PORT = 7000

def parse_klv(buffer):
    """Extrait Key+Length+Value et parse Value en StreamInfo."""
    ok, info = buffer.map(Gst.MapFlags.READ)
    if not ok:
        return None, "Erreur de mapping"
    data = info.data
    buffer.unmap(info)

    if len(data) < 20:
        return None, f"Buffer trop petit ({len(data)} bytes)"

    # On saute les 16 octets de Key
    length = struct.unpack(">I", data[16:20])[0]
    if len(data) < 20 + length:
        return None, f"Payload tronquée : length={length}, dispo={len(data)-20}"

    payload = data[20:20+length]
    msg = info_pb2.StreamInfo()
    try:
        msg.ParseFromString(payload)
        return msg, None
    except Exception as e:
        return None, str(e)

def on_new_klv_sample(sink):
    sample = sink.emit("pull-sample")
    buf = sample.get_buffer()
    # timestamp
    pts = buf.pts
    secs = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1

    msg, err = parse_klv(buf)
    if msg:
        print(f"[{msg.filename}] systemtime={msg.systemtime.seconds}.{msg.systemtime.nanos:09d} session_name{msg.session_name}")
    else:
        print(f"KLV @ {secs:.3f}s → Erreur : {err}")
    return Gst.FlowReturn.OK

def main():
    Gst.init(None)

    pipeline = Gst.parse_launch(
        f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! queue ! tsdemux name=dmx "
        "dmx. ! queue ! meta/x-klv ! queue ! appsink name=klv_sink emit-signals=true drop=true"
    )
    sink = pipeline.get_by_name("klv_sink")
    sink.connect("new-sample", on_new_klv_sample)

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect(
        "message",
        lambda b,m: loop.quit() if m.type in (Gst.MessageType.ERROR, Gst.MessageType.EOS) else None
    )

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
