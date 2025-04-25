
#!/usr/bin/env python3
import os
import struct
import gi
import cv2
import numpy as np
from collections import deque
import info_pb2  # votre protobuf

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration réseau et fenêtres ---
TCP_HOST = "127.0.0.1"
TCP_PORT = 7000
SRC_URI_LEFT = "srt://127.0.0.1:6020?mode=caller"
SRC_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"

cv2.namedWindow("SyncViewLeft", cv2.WINDOW_AUTOSIZE)
cv2.namedWindow("SyncViewRight", cv2.WINDOW_AUTOSIZE)

# --- Buffers circulaires pour métadonnées ---
meta_buffers = {'left': deque(maxlen=200), 'right': deque(maxlen=200)}
last_meta = {'left': None, 'right': None}
FPS=8
TOLERANCE = 1/(2*FPS)  # tolérance en secondes

# --- Parsing KLV ---
def parse_klv(buffer):
    ok, info = buffer.map(Gst.MapFlags.READ)
    if not ok:
        return None, "Erreur mapping"
    data = info.data
    buffer.unmap(info)
    if len(data) < 20:
        return None, "Buffer trop petit"
    length = struct.unpack(">I", data[16:20])[0]
    if len(data) < 20 + length:
        return None, "Payload tronquée"
    payload = data[20:20+length]

    msg = info_pb2.StreamInfo()
    try:
        msg.ParseFromString(payload)
        return msg, None
    except Exception as e:
        return None, str(e)

# --- Callbacks KLV ségrégés ---
def on_new_klv_left_sample(sink):
    sample = sink.emit('pull-sample')
    buf = sample.get_buffer()
    pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
    msg, err = parse_klv(buf)
    if msg and msg.filename != last_meta['left']:
        last_meta['left'] = msg.filename
        meta_buffers['left'].append((pts, msg.filename))
        print(f"KLV LEFT@{pts:.3f}s → {msg.filename}")
    elif err:
        print(f"KLV LEFT@{pts:.3f}s → Erreur: {err}")
    return Gst.FlowReturn.OK


def on_new_klv_right_sample(sink):
    sample = sink.emit('pull-sample')
    buf = sample.get_buffer()
    pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
    msg, err = parse_klv(buf)
    if msg and msg.filename != last_meta['right']:
        last_meta['right'] = msg.filename
        meta_buffers['right'].append((pts, msg.filename))
        print(f"KLV RIGHT@{pts:.3f}s → {msg.filename}")
    elif err:
        print(f"KLV RIGHT@{pts:.3f}s → Erreur: {err}")
    return Gst.FlowReturn.OK

# --- Affichage synchronisé ---
def display_side_by_side(window, video_frame, klv_filename):
    if not klv_filename or not os.path.exists(klv_filename):
        cv2.imshow(window, video_frame)
        cv2.waitKey(1)
        return False

    klv_img = cv2.imread(klv_filename)
    if klv_img is None:
        cv2.imshow(window, video_frame)
        cv2.waitKey(1)
        return False

    h_vid, _ = video_frame.shape[:2]
    h_klv, w_klv = klv_img.shape[:2]
    if h_klv != h_vid:
        scale = h_vid / h_klv
        new_w = int(w_klv * scale)
        klv_img = cv2.resize(klv_img, (new_w, h_vid), interpolation=cv2.INTER_AREA)

    combined = np.hstack((video_frame, klv_img))
    cv2.imshow(window, combined)
    cv2.waitKey(1)
    return False

# --- Callback vidéo générique ---
def make_video_callback(side, window):
    def on_new_video_sample(sink):
        sample = sink.emit('pull-sample')
        buf = sample.get_buffer()
        pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1

        result, mapinfo = buf.map(Gst.MapFlags.READ)
        if not result:
            return Gst.FlowReturn.OK
        caps = sample.get_caps()
        s = caps.get_structure(0)
        w, h = s.get_value('width'), s.get_value('height')
        data = np.ndarray((h, w, 3), dtype=np.uint8, buffer=mapinfo.data)
        frame = cv2.cvtColor(data, cv2.COLOR_RGB2BGR)
        buf.unmap(mapinfo)


        # Recherche du KLV le plus proche temporellement
        candidates = [(abs(mpt - pts), fname) for mpt, fname in meta_buffers[side]]
        klv_fname = None
        if candidates:
            diff, fname = min(candidates, key=lambda x: x[0])
            if diff <= TOLERANCE:
                klv_fname = fname
                # retire les entrées antérieures pour éviter réutilisation
                meta_buffers[side] = deque([(m,f) for m,f in meta_buffers[side] if m > pts], maxlen=200)
                print(f"Video {side}@{pts:.3f}s sync méta diff={diff:.3f}s → {fname}")

        # # Synchronisation KLV
        # klv_fname = None
        # if meta_buffers[side]:
        #     pts_meta, fn = min(meta_buffers[side], key=lambda x: abs(x[0] - pts))
        #     if abs(pts_meta - pts) < TOLERANCE:
        #         klv_fname = fn
        #         print(f"Video {side}@{pts:.3f}s sync méta@{pts_meta:.3f}s → {fn}")

        GLib.idle_add(display_side_by_side, window, frame, klv_fname)
        return Gst.FlowReturn.OK
    return on_new_video_sample

# --- Handler messages GStreamer ---
def on_message(bus, message, loop):
    if message.type == Gst.MessageType.ERROR:
        err, dbg = message.parse_error()
        print(f"[ERROR] {err.message}")
        loop.quit()
    elif message.type == Gst.MessageType.EOS:
        print("[EOS] Fin de flux")
        loop.quit()

# --- Main ---
def main():
    Gst.init(None)

    # Horloge partagée
    clock = Gst.SystemClock.obtain()
    start_time = clock.get_time()

    # Pipeline metadata unique démultiplexé
    meta_launch = (
        f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! queue ! tsdemux name=dmx "
        f"dmx. ! queue ! meta/x-klv,parsed=true,framerate=4/1 ! queue ! appsink name=klv_sink_left emit-signals=true drop=true sync=false "
        f"dmx. ! queue ! meta/x-klv,parsed=true,framerate=4/1 ! queue ! appsink name=klv_sink_right emit-signals=true drop=true sync=false"
    )
    meta_pipeline = Gst.parse_launch(meta_launch)
    meta_pipeline.use_clock(clock)
    meta_pipeline.set_base_time(start_time)

    klv_left = meta_pipeline.get_by_name('klv_sink_left')
    klv_left.connect('new-sample', on_new_klv_left_sample)
    klv_right = meta_pipeline.get_by_name('klv_sink_right')
    klv_right.connect('new-sample', on_new_klv_right_sample)

    # Pipelines vidéo gauche et droite
    video_left = Gst.parse_launch(
        f"srtsrc latency=1000 uri={SRC_URI_LEFT} ! queue ! application/x-rtp,media=video,clock-rate=90000,encoding-name=JPEG,payload=26 "
        "! rtpjpegdepay ! nvjpegdec ! videoconvert ! video/x-raw,format=RGB "
        "! queue ! appsink name=video_left_sink emit-signals=true sync=true"
    )
    video_left.use_clock(clock)
    video_left.set_base_time(start_time)
    sink_left = video_left.get_by_name('video_left_sink')
    sink_left.connect('new-sample', make_video_callback('left', 'SyncViewLeft'))

    video_right = Gst.parse_launch(
        f"srtsrc latency=1000 uri={SRC_URI_RIGHT} ! queue ! application/x-rtp,media=video,clock-rate=90000,encoding-name=JPEG,payload=26 "
        "! rtpjpegdepay ! nvjpegdec ! videoconvert ! video/x-raw,format=RGB "
        "! queue ! appsink name=video_right_sink emit-signals=true sync=true"
    )
    video_right.use_clock(clock)
    video_right.set_base_time(start_time)
    sink_right = video_right.get_by_name('video_right_sink')
    sink_right.connect('new-sample', make_video_callback('right', 'SyncViewRight'))

    # Démarrage et loop
    loop = GLib.MainLoop()
    for p in (meta_pipeline, video_left, video_right):
        bus = p.get_bus()
        bus.add_signal_watch()
        bus.connect('message', lambda b, m: on_message(b, m, loop))
        p.set_state(Gst.State.PLAYING)

    print(f"Streaming LEFT:{SRC_URI_LEFT}, RIGHT:{SRC_URI_RIGHT}, KLV tcp://{TCP_HOST}:{TCP_PORT}")
    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrompu")
    finally:
        for p in (meta_pipeline, video_left, video_right):
            p.set_state(Gst.State.NULL)

if __name__ == "__main__":
    main()
