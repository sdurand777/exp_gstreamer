
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
IMAGE_DIR_RIGHT     = "/home/ivm/escargot/imgs_right_numbered"
IMAGE_DIR_LEFT      = "/home/ivm/escargot/imgs_left_numbered"
PATTERN             = "img%05d.jpg"
FPS                 = 8
# Separate SRT URIs for left and right
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=listener"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=listener"
# Single TCP port for combined KLV
TCP_HOST            = "127.0.0.1"
TCP_PORT            = 7000

# MD5("StreamInfo")
_key = hashlib.md5(b"StreamInfo").digest()
# Nanoseconds per frame
frame_duration = Gst.SECOND // FPS

# --- Internal counters ---
video_indexes = {IMAGE_DIR_LEFT: 1, IMAGE_DIR_RIGHT: 1}
meta_indexes  = {IMAGE_DIR_LEFT: 1, IMAGE_DIR_RIGHT: 1}

# --- Video callback ---
def on_need_data_video(appsrc, length, image_dir):
    idx = video_indexes[image_dir]
    path = os.path.join(image_dir, PATTERN % idx)
    if not os.path.exists(path):
        appsrc.emit('end-of-stream')
        return
    with open(path, 'rb') as f:
        data = f.read()
    buf = Gst.Buffer.new_allocate(None, len(data), None)
    buf.fill(0, data)
    buf.pts = (idx - 1) * frame_duration
    buf.duration = frame_duration
    appsrc.emit('push-buffer', buf)
    video_indexes[image_dir] += 1

# --- Metadata callback generator ---
def make_meta_callback(image_dir):
    def on_need_data_meta(appsrc, length):
        idx = meta_indexes[image_dir]
        info = info_pb2.StreamInfo()
        # Use full path in filename
        info.filename = os.path.join(image_dir, PATTERN % idx)
        now = time.time()
        ts = Timestamp(seconds=int(now), nanos=int((now - int(now)) * 1e9))
        info.systemtime.CopyFrom(ts)
        info.session_name = "Session Offline"
        payload = info.SerializeToString()

        frame = bytearray(_key)
        frame += struct.pack(
            ">I", len(payload)
        ) + payload
        buf = Gst.Buffer.new_wrapped(frame)
        buf.pts = (idx - 1) * frame_duration
        buf.duration = frame_duration
        appsrc.emit('push-buffer', buf)
        meta_indexes[image_dir] += 1
    return on_need_data_meta

# --- Message handler ---
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

    # Left video pipeline
    pipeline_left = Gst.parse_launch(
        (
            f"appsrc name=vid_left caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
            "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc ! rtpjpegpay mtu=1316 ! "
            f"srtserversink uri={VIDEO_SRT_URI_LEFT}"
        )
    )
    src_vid_left = pipeline_left.get_by_name('vid_left')
    src_vid_left.connect('need-data', on_need_data_video, IMAGE_DIR_LEFT)

    # Right video pipeline
    pipeline_right = Gst.parse_launch(
        (
            f"appsrc name=vid_right caps=\"image/jpeg,framerate={FPS}/1\" is-live=true block=true format=time ! "
            "decodebin ! videoconvert ! video/x-raw,format=I420 ! jpegenc ! rtpjpegpay mtu=1316 ! "
            f"srtserversink uri={VIDEO_SRT_URI_RIGHT}"
        )
    )
    src_vid_right = pipeline_right.get_by_name('vid_right')
    src_vid_right.connect('need-data', on_need_data_video, IMAGE_DIR_RIGHT)

    # Combined metadata pipeline with two appsrcs into one mpegtsmux
    meta_launch = (
        f"appsrc name=klv_left caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" is-live=true block=true format=time ! queue ! "
        f"mpegtsmux name=mux ! tcpserversink host={TCP_HOST} port={TCP_PORT} sync=true "
        f"appsrc name=klv_right caps=\"meta/x-klv,parsed=true,framerate={FPS}/1\" is-live=true block=true format=time ! queue ! mux."
    )
    pipeline_meta = Gst.parse_launch(meta_launch)

    # Connect metadata callbacks
    src_meta_left = pipeline_meta.get_by_name('klv_left')
    src_meta_left.connect('need-data', make_meta_callback(IMAGE_DIR_LEFT))
    src_meta_right = pipeline_meta.get_by_name('klv_right')
    src_meta_right.connect('need-data', make_meta_callback(IMAGE_DIR_RIGHT))

    # Main loop and bus
    loop = GLib.MainLoop()
    for pipeline in (pipeline_left, pipeline_right, pipeline_meta):
        bus = pipeline.get_bus()
        bus.add_signal_watch()
        bus.connect('message', lambda b, msg: on_message(b, msg, loop))

    # Start all
    pipeline_left.set_state(Gst.State.PLAYING)
    pipeline_right.set_state(Gst.State.PLAYING)
    pipeline_meta.set_state(Gst.State.PLAYING)
    print(
        f"Streaming LEFT on {VIDEO_SRT_URI_LEFT}, RIGHT on {VIDEO_SRT_URI_RIGHT},"
        f" and combined KLV on tcp://{TCP_HOST}:{TCP_PORT} at {FPS} FPS"
    )

    try:
        loop.run()
    except KeyboardInterrupt:
        print("Interrupted")
    finally:
        for pipeline in (pipeline_left, pipeline_right, pipeline_meta):
            pipeline.set_state(Gst.State.NULL)

if __name__ == '__main__':
    main()
