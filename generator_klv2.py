
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
import hashlib
import struct
from google.protobuf.timestamp_pb2 import Timestamp

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib

# --- Configuration ---
FPS           = 10
TCP_HOST      = "127.0.0.1"
TCP_PORT      = 7000

# Calcul de la Key MD5("StreamInfo")
_key = hashlib.md5(b"StreamInfo").digest()  # 16 octets

# --- État interne ---
meta_index     = 1
frame_duration = Gst.SECOND // FPS

def on_need_data_meta(appsrc, length):
    global meta_index

    # Construction du StreamInfo protobuf
    info = info_pb2.StreamInfo()
    info.filename = f"{meta_index:06d}.jpg"
    now = time.time()
    ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
    info.systemtime.CopyFrom(ts)
    info.session_name="Session Offline" 
    payload = info.SerializeToString()

    # Préfixe Key + Length + payload
    klv_packet = bytearray()
    klv_packet += _key
    klv_packet += struct.pack(">I", len(payload))
    klv_packet += payload

    # Pousser dans GStreamer
    buf = Gst.Buffer.new_wrapped(klv_packet)
    buf.pts = frame_duration * (meta_index - 1)
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    meta_index += 1

def on_message(bus, message, loop):
    if message.type == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        loop.quit()
    elif message.type == Gst.MessageType.EOS:
        loop.quit()

def main():
    Gst.init(None)
    pipeline = Gst.parse_launch(
        f"appsrc name=klvsrc is-live=true block=false format=time do-timestamp=true "
        "caps=\"meta/x-klv,parsed=(boolean)true\" ! "
        f"queue ! mpegtsmux ! tcpserversink host={TCP_HOST} port={TCP_PORT} sync=false"
    )
    klvsrc = pipeline.get_by_name('klvsrc')
    klvsrc.connect('need-data', on_need_data_meta)

    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect('message', on_message, loop)

    pipeline.set_state(Gst.State.PLAYING)
    print(f"Serveur KLV-TS démarré sur tcp://{TCP_HOST}:{TCP_PORT}")
    try:
        loop.run()
    except KeyboardInterrupt:
        pass
    finally:
        pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()
