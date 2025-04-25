
#!/usr/bin/env python3
import sys
import os
import struct
import gi
import cv2
import numpy as np
from collections import deque
import info_pb2  # votre protobuf

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration réseau et chemin local des images ---
TCP_HOST = "127.0.0.1"
TCP_PORT = 7000
SRC_URI = "srt://127.0.0.1:6020?mode=caller"
IMAGE_DIR = "/home/ivm/escargot/imgs"

# --- Buffer circulaire pour stocker les métadonnées KLV ---
meta_buffer = deque(maxlen=200)
last_meta_filename = None
#TOLERANCE = 0.05  # tolérance en secondes pour la synchronisation
TOLERANCE = 0.20  # tolérance en secondes pour la synchronisation

# --- Fenêtre OpenCV pour affichage concaténé ---
cv2.namedWindow("SyncView", cv2.WINDOW_AUTOSIZE)


def display_side_by_side(video_frame, klv_filename):
    """
    Affiche la vidéo seule si pas de KLV ou concatène la vidéo et l'image KLV sur une même fenêtre.
    """
    if klv_filename is None:
        cv2.imshow("SyncView", video_frame)
        cv2.waitKey(1)
        return False

    #klv_path = os.path.join(IMAGE_DIR, "img" + klv_filename)
    klv_path = klv_filename
    klv_img = cv2.imread(klv_path)
    if klv_img is None:
        cv2.imshow("SyncView", video_frame)
        cv2.waitKey(1)
        return False

    h_vid, _ = video_frame.shape[:2]
    h_klv, w_klv = klv_img.shape[:2]
    if h_klv != h_vid:
        scale = h_vid / h_klv
        new_w = int(w_klv * scale)
        klv_img = cv2.resize(klv_img, (new_w, h_vid), interpolation=cv2.INTER_AREA)

    combined = np.hstack((video_frame, klv_img))
    cv2.imshow("SyncView", combined)
    cv2.waitKey(1)
    return False


def parse_klv(buffer):
    ok, info = buffer.map(Gst.MapFlags.READ)
    if not ok:
        return None, "Erreur de mapping"
    data = info.data
    buffer.unmap(info)

    if len(data) < 20:
        return None, f"Buffer trop petit ({len(data)} bytes)"

    length = struct.unpack(">I", data[16:20])[0]
    if len(data) < 20 + length:
        return None, f"Payload tronquée : length={length}, dispo={len(data)-20}"

    payload = data[20:20+length]
    msg = info_pb2.StreamInfo()
    try:
        msg.ParseFromString(payload)
        return msg, None
    except Exception as e:
        return None, str(e)


def on_new_klv_sample(sink):
    global last_meta_filename
    sample = sink.emit("pull-sample")
    buf = sample.get_buffer()
    pts_s = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
    msg, err = parse_klv(buf)
    if msg and msg.filename != last_meta_filename:
        last_meta_filename = msg.filename
        meta_buffer.append((pts_s, msg.filename))
        print(f"KLV @ {pts_s:.3f}s → filename={msg.filename}")
    elif err:
        print(f"KLV @ {pts_s:.3f}s → Erreur: {err}")
    return Gst.FlowReturn.OK


def on_new_video_sample(sink):
    sample = sink.emit('pull-sample')
    buf = sample.get_buffer()
    pts_s = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
    print(f"Video @ {pts_s:.3f}s")

    result, mapinfo = buf.map(Gst.MapFlags.READ)
    if not result:
        return Gst.FlowReturn.OK
    caps = sample.get_caps()
    s = caps.get_structure(0)
    w, h = s.get_value('width'), s.get_value('height')
    video_data = np.ndarray((h, w, 3), dtype=np.uint8, buffer=mapinfo.data)
    video_bgr = cv2.cvtColor(video_data, cv2.COLOR_RGB2BGR)
    buf.unmap(mapinfo)

    klv_filename = None
    if meta_buffer:
        closest_pts, fname = min(meta_buffer, key=lambda x: abs(x[0] - pts_s))
        if abs(closest_pts - pts_s) < TOLERANCE:
            klv_filename = fname
            print(f"  → Sync match: méta {closest_pts:.3f}s → {fname}")

    GLib.idle_add(display_side_by_side, video_bgr, klv_filename)
    return Gst.FlowReturn.OK


def on_message(bus, message, loop):
    if message.type == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        if dbg:
            print("Debug:", dbg)
        loop.quit()
    elif message.type == Gst.MessageType.EOS:
        print("[EOS] End of stream")
        loop.quit()


def main():
    Gst.init(None)

    # Créer et partager une horloge
    clock = Gst.SystemClock.obtain()
    start_time = clock.get_time()

    # Pipeline KLV
    meta_pipeline = Gst.parse_launch(
        f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! queue ! tsdemux name=dmx "
        "dmx. ! queue ! meta/x-klv ! queue ! appsink name=klv_sink emit-signals=true drop=true sync=true"
    )
    meta_pipeline.use_clock(clock)
    meta_pipeline.set_base_time(start_time)
    klv_sink = meta_pipeline.get_by_name('klv_sink')
    klv_sink.connect('new-sample', on_new_klv_sample)

    # Pipeline Vidéo SRT
    video_pipeline = Gst.parse_launch(
        f"srtsrc latency=1000 uri={SRC_URI} ! queue ! application/x-rtp,media=video,clock-rate=90000,encoding-name=JPEG,payload=26 "
        "! rtpjpegdepay ! nvjpegdec ! videoconvert ! video/x-raw,format=RGB "
        "! queue ! appsink name=video_sink emit-signals=true sync=true"
    )
    video_pipeline.use_clock(clock)
    video_pipeline.set_base_time(start_time)
    video_sink = video_pipeline.get_by_name('video_sink')
    video_sink.connect('new-sample', on_new_video_sample)

    # Boucle principale et bus
    loop = GLib.MainLoop()
    for pipe in (meta_pipeline, video_pipeline):
        bus = pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', lambda b, m: on_message(b, m, loop))
        pipe.set_state(Gst.State.PLAYING)

    print(f"Streaming vidéo sur {SRC_URI} et KLV sur tcp://{TCP_HOST}:{TCP_PORT}")
    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrompu par l'utilisateur")
    finally:
        meta_pipeline.set_state(Gst.State.NULL)
        video_pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    main()
