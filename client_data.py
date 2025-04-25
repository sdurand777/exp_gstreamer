
#!/usr/bin/env python3
import sys
import os
import struct
import gi
import cv2
import numpy as np
import re
import collections

# votre protobuf
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

TCP_HOST = "127.0.0.1"
TCP_PORT = 7000
# Path to local images for verification
IMAGE_DIR = "/home/ivm/escargot/imgs"

# --- Variables for duplicate filtering ---
last_meta_filename = None

# --- Fonction d'affichage en idle ---
cv2.namedWindow("frame", cv2.WINDOW_AUTOSIZE)
cv2.namedWindow("meta_image", cv2.WINDOW_AUTOSIZE)

def display_frame(frame):
    cv2.imshow("frame", frame)
    cv2.waitKey(1)
    return False

def display_meta_image(filename):
    img_path = os.path.join(IMAGE_DIR, "img"+filename)
    frame = cv2.imread(img_path)
    if frame is not None:
        cv2.imshow("meta_image", frame)
        cv2.waitKey(1)
    return False  # Remove idle source after execution

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

    pts = buf.pts
    pts_time = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1

    msg, err = parse_klv(buf)
    if msg:
        # Filter duplicates by filename
        if msg.filename != last_meta_filename:
            last_meta_filename = msg.filename
            print("on new klv sample")
            print(f"####### PTS KLV : {pts_time:.3f}")
            print(f"[{msg.filename}] systemtime={msg.systemtime.seconds}.{msg.systemtime.nanos:09d} session_name={msg.session_name}")
            # Display corresponding image for sync check
            # Schedule image display via GLib
            GLib.idle_add(display_meta_image, msg.filename)
    else:
        print(f"KLV @ {pts_time:.3f}s → Erreur : {err}")
    return Gst.FlowReturn.OK


def on_new_video_sample(appsink):
    sample = appsink.emit('pull-sample')
    buf = sample.get_buffer()
    pts = buf.pts
    pts_time = pts / Gst.SECOND if pts != Gst.CLOCK_TIME_NONE else -1
    print("on new video sample")
    print(f"========== PTS VIDEO : {pts_time:.3f}")

    result, mapinfo = buf.map(Gst.MapFlags.READ)
    if not result:
        return Gst.FlowReturn.OK

    caps = sample.get_caps()
    structure = caps.get_structure(0)
    width = structure.get_value('width')
    height = structure.get_value('height')
    pixel_format = structure.get_value('format')
    print(f"processing image: {width}x{height}, format: {pixel_format}")

    if pixel_format == 'RGB':
        frame_data = np.ndarray((height, width, 3), dtype=np.uint8, buffer=mapinfo.data)
        frame_bgr = cv2.cvtColor(frame_data, cv2.COLOR_RGB2BGR)
        GLib.idle_add(display_frame, frame_bgr)

    buf.unmap(mapinfo)
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

    # Pipeline metadata
    meta_pipeline = Gst.parse_launch(
        f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! queue ! tsdemux name=dmx "
        "dmx. ! queue ! meta/x-klv ! queue ! appsink name=klv_sink emit-signals=true drop=true sync=true"
    )
    klv_sink = meta_pipeline.get_by_name("klv_sink")
    klv_sink.connect("new-sample", on_new_klv_sample)

    # Pipeline vidéo
    SRC_URI = "srt://127.0.0.1:6020?mode=caller"
    video_pipeline = Gst.parse_launch(
        f"srtsrc latency=1000 uri={SRC_URI} ! application/x-rtp,media=video,clock-rate=90000,encoding-name=JPEG,payload=26 ! "
        "queue ! rtpjpegdepay ! nvjpegdec ! queue ! videoconvert ! video/x-raw,format=RGB ! "
        "appsink name=video_sink emit-signals=true sync=true"
    )
    video_sink = video_pipeline.get_by_name('video_sink')
    video_sink.connect('new-sample', on_new_video_sample)

    loop = GLib.MainLoop()
    for pipeline in (meta_pipeline, video_pipeline):
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message', lambda b, m: on_message(b, m, loop))

    meta_pipeline.set_state(Gst.State.PLAYING)
    video_pipeline.set_state(Gst.State.PLAYING)

    print(f"Streaming video on {SRC_URI} and KLV on tcp://{TCP_HOST}:{TCP_PORT}")
    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        meta_pipeline.set_state(Gst.State.NULL)
        video_pipeline.set_state(Gst.State.NULL)

if __name__ == "__main__":
    main()
