
#!/usr/bin/env python3
import os
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GObject, GLib

# --- Configuration ---
IMAGE_DIR     = "/home/ivm/escargot/imgs"
PATTERN       = "img%05d.jpg"
FPS           = 1
VIDEO_SRT_URI = "srt://127.0.0.1:6020?mode=listener"
TCP_HOST      = "127.0.0.1"
TCP_PORT      = 7000

# MD5("StreamInfo")
_key = hashlib.md5(b"StreamInfo").digest()

# Frame duration in nanoseconds
frame_duration = Gst.SECOND // FPS

# --- Callbacks ---
def on_need_data_meta(appsrc, length):
    global meta_index

    # Build StreamInfo protobuf
    info = info_pb2.StreamInfo()
    info.filename = f"{meta_index:06d}.jpg"
    now = time.time()
    ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
    info.systemtime.CopyFrom(ts)
    info.session_name = "Session Offline"
    payload = info.SerializeToString()

    # Prefix key + length + payload
    klv_packet = bytearray(_key)
    klv_packet += struct.pack(">I", len(payload))
    klv_packet += payload

    # Push into appsrc
    buf = Gst.Buffer.new_wrapped(klv_packet)
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    meta_index += 1


def on_need_data_video(appsrc, length):
    global video_index
    path = os.path.join(IMAGE_DIR, PATTERN % video_index)
    if not os.path.exists(path):
        appsrc.emit('end-of-stream')
        return

    data = open(path, 'rb').read()
    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)

    video_index += 1


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
    GObject.threads_init()

    # Video pipeline (SRT)
    video_pipeline = Gst.parse_launch(
        f"appsrc name=video_src caps=\"image/jpeg,framerate={FPS}/1\" is-live=true format=time do-timestamp=true ! "
        "decodebin ! videoconvert ! video/x-raw,format=I420 ! "
        "jpegenc ! rtpjpegpay mtu=1316 ! "
        f"srtserversink uri={VIDEO_SRT_URI}"
    )
    video_src = video_pipeline.get_by_name('video_src')
    video_src.connect('need-data', on_need_data_video)

    # Metadata pipeline (KLV via MPEG-TS over TCP)
    meta_pipeline = Gst.parse_launch(
        f"appsrc name=klvsrc is-live=true format=time do-timestamp=true caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" ! "
        "queue ! mpegtsmux ! tcpserversink "
        f"host={TCP_HOST} port={TCP_PORT}"
    )
    klvsrc = meta_pipeline.get_by_name('klvsrc')
    klvsrc.connect('need-data', on_need_data_meta)

    loop = GLib.MainLoop()
    for pipeline in (video_pipeline, meta_pipeline):
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message', on_message, loop)

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
    meta_index = 1
    video_index = 1
    import time
    main()
