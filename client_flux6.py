
#!/usr/bin/env python3
import os
import time
import gi
import info_pb2
from google.protobuf.timestamp_pb2 import Timestamp
import hashlib
import struct
import threading
import queue
import numpy as np
import cv2
from collections import defaultdict

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
IMAGE_DIR_RIGHT     = "/home/smith/dataset/sequences/00/image_0/jpgs_numbered"
IMAGE_DIR_LEFT      = "/home/smith/dataset/sequences/00/image_1/jpgs_numbered"
PATTERN             = "%05d.jpg"
FPS                 = 8
VIDEO_SRT_URI_LEFT  = "srt://127.0.0.1:6020?mode=caller"
VIDEO_SRT_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"
TCP_HOST            = "127.0.0.1"
TCP_PORT            = 7000

# SKIP first seconds to align the data
SKIP = 3


# Initialize GStreamer and keys/indexes
Gst.init(None)
_key = hashlib.md5(b"StreamInfo").digest()
frame_duration = Gst.SECOND // FPS

class SRTSyncClient:
    def __init__(self, fps=8):
        # Main loop and threading
        self.loop = GLib.MainLoop()
        self.sample_queue = queue.Queue()
        self.sync_buffer = defaultdict(lambda: {
            'frame_left': None, 'klv_left': None,
            'frame_right': None, 'klv_right': None,
            'pts_frame_left': None, 'pts_klv_left': None,
            'pts_frame_right': None, 'pts_klv_right': None
        })
        self.global_offset = None
        self.pts_precision = 1.0 / fps
        self.running = True
        self.start_pts_video = None

        # Start sample processing thread
        self.processor_thread = threading.Thread(target=self._process_samples, daemon=True)
        self.processor_thread.start()

        # Build and start pipelines
        self.video_pipes = {}
        self._build_meta_pipeline()
        self._build_video_pipeline('left',  VIDEO_SRT_URI_LEFT)
        self._build_video_pipeline('right', VIDEO_SRT_URI_RIGHT)

    def _build_meta_pipeline(self):
        desc = (
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=demux "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_left emit-signals=true sync=false drop=false "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_right emit-signals=true sync=false drop=false"
        )
        self.meta_pipe = Gst.parse_launch(desc)
        self.meta_pipe.get_by_name('klv_left').connect('new-sample',  self._on_meta('left'))
        self.meta_pipe.get_by_name('klv_right').connect('new-sample', self._on_meta('right'))
        bus = self.meta_pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        self.meta_pipe.set_state(Gst.State.PLAYING)

    def _build_video_pipeline(self, side, uri):
        desc = (
            f"srtsrc latency=1000 uri={uri} ! queue ! "
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! jpegparse ! "
            f"appsink name=vid_{side} caps=\"image/jpeg\" emit-signals=true sync=false drop=false"
        )
        pipe = Gst.parse_launch(desc)
        pipe.get_by_name(f'vid_{side}').connect('new-sample', self._on_video(side))
        bus = pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        pipe.set_state(Gst.State.PLAYING)
        self.video_pipes[side] = pipe

    def _on_meta(self, side):
        def handler(sink):
            sample = sink.emit('pull-sample')
            buf = sample.get_buffer()
            pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
            # skip first 2 secs
            if pts <= SKIP:
                return Gst.FlowReturn.OK

            # skip meta until first video arrives
            if self.start_pts_video is None or pts < self.start_pts_video:
                return Gst.FlowReturn.OK
            ok, info = buf.map(Gst.MapFlags.READ)
            fname = None
            if ok:
                data = info.data
                buf.unmap(info)
                if len(data) >= 20:
                    length = struct.unpack('>I', data[16:20])[0]
                    payload = data[20:20+length]
                    msg = info_pb2.StreamInfo()
                    try:
                        msg.ParseFromString(payload)
                        fname = msg.filename
                    except:
                        pass
            reduced = self._calc_reduced_pts(pts)
            if fname:
                self.sample_queue.put(('klv', side, fname, pts, reduced))
            return Gst.FlowReturn.OK
        return handler

    def _on_video(self, side):
        def handler(sink):
            sample = sink.emit('pull-sample')
            buf = sample.get_buffer()
            pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
            # skip first 2 secs
            if pts <= SKIP:
                return Gst.FlowReturn.OK

            # set start_pts_video at first frame
            if self.start_pts_video is None:
                self.start_pts_video = pts
            ok, info = buf.map(Gst.MapFlags.READ)
            frame_img = None
            if ok:
                data = info.data
                buf.unmap(info)
                np_arr = np.frombuffer(data, dtype=np.uint8)
                frame_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
            reduced = self._calc_reduced_pts(pts)
            self.sample_queue.put(('frame', side, frame_img, pts, reduced))
            return Gst.FlowReturn.OK
        return handler

    def _calc_reduced_pts(self, pts_time):
        if pts_time < 0:
            return -1
        if self.global_offset is None:
            down = int(pts_time / self.pts_precision) * self.pts_precision
            self.global_offset = pts_time - down
        else:
            down = round((pts_time - self.global_offset) / self.pts_precision) * self.pts_precision
            new_offset = pts_time - down
            self.global_offset = 0.8 * self.global_offset + 0.2 * new_offset
        aligned = pts_time - self.global_offset
        return round(aligned / self.pts_precision) * self.pts_precision

    def _process_samples(self):
        while self.running:
            try:
                typ, side, data, pts, reduced = self.sample_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            entry = self.sync_buffer[reduced]
            key = f'{typ}_{side}'
            entry[key] = data if typ=='frame' else data
            entry[f'pts_{typ}_{side}'] = pts
            if all(entry[k] is not None for k in ('frame_left','frame_right','klv_left','klv_right')):

                #print(f"[SYNC] PTS: {reduced:.3f}s | KLV_LEFT: {entry['klv_left']} | KLV_RIGHT: {entry['klv_right']}")
                print(f"""[SYNC]
                Reduced PTS : {reduced:.3f}s
                Frame Left  : PTS={entry['pts_frame_left']:.3f}s
                Frame Right : PTS={entry['pts_frame_right']:.3f}s
                KLV Left    : PTS={entry['pts_klv_left']:.3f}s | File={entry['klv_left']}
                KLV Right   : PTS={entry['pts_klv_right']:.3f}s | File={entry['klv_right']}
                """)

                left = entry['frame_left']
                right= entry['frame_right']
                if left is not None and right is not None:
                    concat = np.hstack((left, right))
                    img_name = f"{reduced:.3f}_L_{entry['klv_left']}_R_{entry['klv_right']}.png".replace('/','_')
                    cv2.imwrite(os.path.join('save', img_name), concat)
                del self.sync_buffer[reduced]

    def _on_message(self, bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err, dbg = msg.parse_error()
            print("[ERROR]", err.message)
            self._cleanup()
            self.loop.quit()
        elif msg.type == Gst.MessageType.EOS:
            self._cleanup()
            self.loop.quit()

    def _cleanup(self):
        self.running = False
        for p in self.video_pipes.values(): p.set_state(Gst.State.NULL)
        self.meta_pipe.set_state(Gst.State.NULL)

    def run(self):
        print(f"Streaming LEFT {VIDEO_SRT_URI_LEFT}, RIGHT {VIDEO_SRT_URI_RIGHT}, KLV tcp://{TCP_HOST}:{TCP_PORT}")
        try:
            self.loop.run()
        except KeyboardInterrupt:
            pass
        finally:
            self._cleanup()

if __name__ == '__main__':
    app = SRTSyncClient(fps=FPS)
    app.run()
