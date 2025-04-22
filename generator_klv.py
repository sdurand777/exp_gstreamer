
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
from gi.repository import Gst, GObject, GLib

gi.require_version('Gst', '1.0')

# --- Configuration ---
PATTERN       = "%06d.jpg"          # juste pour générer un nom de fichier
FPS           = 10
TCP_HOST      = "127.0.0.1"
TCP_PORT      = 7000

# --- État interne ---
meta_index     = 1
frame_duration = Gst.SECOND // FPS  # durée d’une « frame » pour l’horodatage

def on_need_data_meta(appsrc, length):
    global meta_index
    filename = PATTERN % meta_index

    # Construction du message protobuf
    info = info_pb2.StreamInfo()
    info.filename = filename
    now = time.time()
    ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
    info.systemtime.CopyFrom(ts)

    # Sérialisation
    payload = info.SerializeToString()
    buf = Gst.Buffer.new_allocate(None, len(payload), None)
    buf.fill(0, payload)
    buf.pts = frame_duration * (meta_index - 1)
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    meta_index += 1

def on_message(bus, message, loop):
    t = message.type
    if t == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        if dbg: print("Debug:", dbg)
        loop.quit()
    elif t == Gst.MessageType.EOS:
        print("[EOS] End of stream")
        loop.quit()

def main():
    Gst.init(None)
    GObject.threads_init()

    # Pipeline KLV via MPEG-TS sur TCP
    pipeline = Gst.parse_launch(
        f"appsrc name=klvsrc is-live=true block=false format=time do-timestamp=true "
        f"caps=\"meta/x-klv,parsed=(boolean)true\" ! "
        "queue ! mpegtsmux ! "
        f"tcpserversink host={TCP_HOST} port={TCP_PORT} recover-policy=keyframe sync=false"
    )
    klvsrc = pipeline.get_by_name('klvsrc')
    klvsrc.connect('need-data', on_need_data_meta)

    # Boucle GLib
    loop = GLib.MainLoop()
    bus = pipeline.get_bus()
    bus.add_signal_watch()
    bus.connect('message', on_message, loop)

    pipeline.set_state(Gst.State.PLAYING)
    print(f"Serveur KLV-TS à tcp://{TCP_HOST}:{TCP_PORT}")

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrompu par l'utilisateur")
    finally:
        pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()
