
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib

# --- Configuration ---
#IMAGE_DIR     = "/home/smith/dataset/sequences/00/image_0/jpgs"
IMAGE_DIR     = "/home/ivm/escargot/imgs"
#PATTERN       = "%06d.jpg"          # ex. 000001.jpg, 000002.jpg, …
PATTERN       = "img%05d.jpg"          # ex. 000001.jpg, 000002.jpg, …
FPS           = 1
VIDEO_SRT_URI = "srt://127.0.0.1:6020?mode=listener"

# TCP settings for KLV over MPEG-TS
TCP_HOST      = "127.0.0.1"
TCP_PORT      = 7000

# Calcul de la Key MD5("StreamInfo")
_key = hashlib.md5(b"StreamInfo").digest()  # 16 octets

# --- Internal state ---
video_index    = 1
meta_index     = 1
timestamp_ns   = 0
timestamp_ns_meta = 0
frame_duration = Gst.SECOND // FPS  # nanoseconds per frame

# --- Callbacks ---
def on_need_data_meta(appsrc, length):
    global meta_index, timestamp_ns_meta

    # → on ralentit ici pour ne pas dépasser 1 image par seconde
    time.sleep(1.0 / FPS)

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
    # buf.pts = frame_duration * (meta_index - 1)
    # buf.duration = frame_duration

    buf.pts = timestamp_ns_meta
    buf.duration = frame_duration

    appsrc.emit('push-buffer', buf)

    timestamp_ns_meta += frame_duration

    meta_index += 1


# --- Callbacks ---
def on_need_data_video(appsrc, length):
    global video_index, timestamp_ns
    path = os.path.join(IMAGE_DIR, PATTERN % video_index)
    if not os.path.exists(path):
        appsrc.emit('end-of-stream')
        return

    data = open(path, 'rb').read()
    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.pts = timestamp_ns
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    timestamp_ns += frame_duration
    video_index += 1


# def on_need_data_meta(appsrc, length):
#     global meta_index
#     filename = PATTERN % meta_index
#     # Build StreamInfo
#     info = info_pb2.StreamInfo()
#     info.filename = filename
#     now = time.time()
#     ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
#     info.systemtime.CopyFrom(ts)

#     payload = info.SerializeToString()
#     buf = Gst.Buffer.new_allocate(None, len(payload), None)
#     buf.fill(0, payload)
#     buf.pts = frame_duration * (meta_index - 1)
#     buf.duration = frame_duration
#     appsrc.emit('push-buffer', buf)

#     meta_index += 1


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

# --- Main ---
def main():
    Gst.init(None)
    GObject.threads_init()

    # Video pipeline (SRT)
    video_pipeline = Gst.parse_launch(
        f"appsrc name=video_src caps=\"image/jpeg,framerate={FPS}/1\" is-live=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! "
        "jpegenc ! rtpjpegpay mtu=1316 ! "
        f"srtserversink uri={VIDEO_SRT_URI}"
    )
    video_src = video_pipeline.get_by_name('video_src')
    video_src.connect('need-data', on_need_data_video)

    # Metadata pipeline (KLV via MPEG-TS over TCP)
    # meta_pipeline_desc = (
    #     f"appsrc name=klv_src is-live=true block=false format=time do-timestamp=true "
    #     f"caps=\"meta/x-klv,parsed=(boolean)true\" ! queue ! "
    #     f"mpegtsmux name=mux ! queue ! "
    #     f"tcpserversink host={TCP_HOST} port={TCP_PORT} sync=false recover-policy=keyframe"
    # )
    # meta_pipeline = Gst.parse_launch(meta_pipeline_desc)
    # meta_pipeline = Gst.parse_launch(
    #     f"appsrc name=klvsrc is-live=true block=false format=time do-timestamp=true "
    #     f"caps=\"meta/x-klv,parsed=(boolean)true\" ! "
    #     f"queue ! mpegtsmux name=mux ! queue ! "
    #     f"tcpserversink host={TCP_HOST} port={TCP_PORT} recover-policy=keyframe sync=false"
    # )

    meta_pipeline = Gst.parse_launch(
        f"appsrc name=klvsrc "
        #f"is-live=true block=true format=time do-timestamp=true "
        f"is-live=true format=time "
        # on indique ici la cadence
        f"caps=\"meta/x-klv,parsed=(boolean)true,framerate={FPS}/1\" ! "
        "queue ! mpegtsmux ! tcpserversink "
        f"host={TCP_HOST} port={TCP_PORT} sync=false"
        )

    klvsrc = meta_pipeline.get_by_name('klvsrc')
    klvsrc.connect('need-data', on_need_data_meta)

    # GLib loop
    loop = GLib.MainLoop()
    for pipeline in (video_pipeline, meta_pipeline):
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message', on_message, loop)

    # Start pipelines
    video_pipeline.set_state(Gst.State.PLAYING)
    meta_pipeline.set_state(Gst.State.PLAYING)
    print(f"Streaming video (SRT) on {VIDEO_SRT_URI} and KLV-TS (TCP) on tcp://{TCP_HOST}:{TCP_PORT}")

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted by user")
    finally:
        video_pipeline.set_state(Gst.State.NULL)
        meta_pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()
