
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
IMAGE_DIR     = "/home/ivm/escargot/imgs"
PATTERN       = "img%05d.jpg"
FPS           = 4
VIDEO_SRT_URI = "srt://127.0.0.1:6020?mode=listener"
TCP_HOST      = "127.0.0.1"
TCP_PORT      = 7000

# MD5("StreamInfo")
_key = hashlib.md5(b"StreamInfo").digest()
# Nanoseconds per frame
frame_duration = Gst.SECOND // FPS

# --- Internal counters ---
video_index = 1
meta_index  = 1

# --- Callbacks ---
def on_need_data_video(appsrc, length):
    global video_index
    # Push one frame
    path = os.path.join(IMAGE_DIR, PATTERN % video_index)
    if not os.path.exists(path):
        appsrc.emit('end-of-stream')
        return
    data = open(path, 'rb').read()
    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.pts = (video_index - 1) * frame_duration
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)
    video_index += 1


def on_need_data_meta(appsrc, length):
    global meta_index
    # Push one KLV packet
    info = info_pb2.StreamInfo()
    info.filename = f"{meta_index:05d}.jpg"
    now = time.time()
    ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
    info.systemtime.CopyFrom(ts)
    info.session_name = "Session Offline"
    payload = info.SerializeToString()

    frame = bytearray(_key)
    frame += struct.pack(">I", len(payload))
    frame += payload
    buf = Gst.Buffer.new_wrapped(frame)
    buf.pts = (meta_index - 1) * frame_duration
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    meta_index += 1


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

    # Video pipeline (SRT)
    video_pipeline = Gst.parse_launch(
        f"appsrc name=video_src caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc ! rtpjpegpay mtu=1316 ! "
        f"srtserversink uri={VIDEO_SRT_URI}"
    )
    video_src = video_pipeline.get_by_name('video_src')
    video_src.connect('need-data', on_need_data_video)

    # Metadata pipeline (KLV over MPEG-TS via TCP)
    meta_pipeline = Gst.parse_launch(
        f"appsrc name=klvsrc caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" is-live=true block=true format=time ! "
        "queue ! mpegtsmux ! tcpserversink "
        f"host={TCP_HOST} port={TCP_PORT} sync=true"
    )
    klvsrc = meta_pipeline.get_by_name('klvsrc')
    klvsrc.connect('need-data', on_need_data_meta)

    # Main loop
    loop = GLib.MainLoop()
    for pipe in (video_pipeline, meta_pipeline):
        bus = pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', lambda b, msg: on_message(b, msg, loop))

    # Start
    video_pipeline.set_state(Gst.State.PLAYING)
    meta_pipeline.set_state(Gst.State.PLAYING)
    print(f"Streaming on {VIDEO_SRT_URI} & tcp://{TCP_HOST}:{TCP_PORT} at {FPS} FPS")

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        video_pipeline.set_state(Gst.State.NULL)
        meta_pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()
