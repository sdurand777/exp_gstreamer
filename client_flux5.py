
#!/usr/bin/env python3
import struct
import gi
import threading
import queue
import os
import numpy as np
import cv2
from collections import defaultdict
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
TCP_HOST      = "127.0.0.1"
TCP_PORT      = 7000
SRC_URI_LEFT  = "srt://127.0.0.1:6020?mode=caller"
SRC_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"

class SRTSyncClient:
    def __init__(self, fps=8):
        # Initialize GStreamer
        Gst.init(None)
        self.loop = GLib.MainLoop()

        # Ensure save directory exists
        os.makedirs('save', exist_ok=True)

        # Thread-safe queue for samples
        self.sample_queue = queue.Queue()
        # Buffer for grouping by reduced PTS
        self.sync_buffer = defaultdict(lambda: {
            'frame_left': None, 'klv_left': None,
            'frame_right': None, 'klv_right': None,
            'pts_frame_left': None, 'pts_klv_left': None,
            'pts_frame_right': None, 'pts_klv_right': None
        })

        # PTS sync variables (global offset, precision)
        self.global_offset = None
        self.pts_precision = 1.0 / fps

        # Start processing thread
        self.running = True
        self.processor_thread = threading.Thread(target=self._process_samples, daemon=True)
        self.processor_thread.start()

        # Build and launch pipelines
        self._build_meta_pipeline()
        self.video_pipes = {}
        self._build_video_pipeline('left', SRC_URI_LEFT)
        self._build_video_pipeline('right', SRC_URI_RIGHT)

    def _build_meta_pipeline(self):
        desc = (
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=demux "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_left emit-signals=true sync=false "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_right emit-signals=true sync=false "
        )
        pipe = Gst.parse_launch(desc)
        pipe.get_by_name('klv_left').connect('new-sample', self._on_meta('left'))
        pipe.get_by_name('klv_right').connect('new-sample', self._on_meta('right'))
        bus = pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        pipe.set_state(Gst.State.PLAYING)
        self.meta_pipe = pipe

    def _build_video_pipeline(self, side, uri):
        desc = (
            f"srtsrc latency=1000 uri={uri} ! queue ! "
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! jpegparse ! "
            f"appsink name=vid_{side} caps=\"image/jpeg\" emit-signals=true sync=true max-buffers=1 drop=true"
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
            fname = self._parse_klv(buf)

            if side == "left":
                print("############### pts left meta =", pts, "fname", fname)
            else:
                print("############### pts right meta =", pts, "fname", fname)

            if fname:
                reduced = self._calc_reduced_pts(pts)
                self.sample_queue.put(('klv', side, fname, pts, reduced))
            return Gst.FlowReturn.OK
        return handler

    def _on_video(self, side):
        def handler(sink):
            sample = sink.emit('pull-sample')
            buf = sample.get_buffer()
            pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1

            if side == "left":
                print("============= pts left video =", pts)
            else:
                print("============= pts right video =", pts)

            reduced = self._calc_reduced_pts(pts)

            ok, info = buf.map(Gst.MapFlags.READ)
            frame_img = None
            if ok:
                data = info.data
                buf.unmap(info)
                # Decode JPEG to BGR image
                np_arr = np.frombuffer(data, dtype=np.uint8)
                frame_img = cv2.imdecode(np_arr, cv2.IMREAD_COLOR)
                if frame_img is None:
                    print("[WARN] échec imdecode JPEG")
            else:
                print("[WARN] buffer.map failed")

            self.sample_queue.put(('frame', side, frame_img, pts, reduced))
            return Gst.FlowReturn.OK
        return handler

    def _parse_klv(self, buf):
        ok, info = buf.map(Gst.MapFlags.READ)
        if not ok:
            return None
        data = info.data
        buf.unmap(info)
        if len(data) < 20:
            return None
        length = struct.unpack('>I', data[16:20])[0]
        payload = data[20:20+length]
        msg = info_pb2.StreamInfo()
        try:
            msg.ParseFromString(payload)
            return msg.filename
        except Exception:
            return None

    def _calc_reduced_pts(self, pts_time):
        if pts_time < 0:
            return -1
        if self.global_offset is None:
            down = int(pts_time / self.pts_precision) * self.pts_precision
            self.global_offset = pts_time - down
        else:
            down = round((pts_time - self.global_offset) / self.pts_precision) * self.pts_precision
            new_offset = pts_time - down
            current = self.global_offset
            updated_offset = 0.8 * current + 0.2 * new_offset
            if abs(updated_offset - current) > 0.01:
                self.global_offset = updated_offset
                print(f"Offset updated: old={current:.6f}s new={updated_offset:.6f}s")
        aligned = pts_time - self.global_offset
        return round(aligned / self.pts_precision) * self.pts_precision

    def _process_samples(self):
        while self.running:
            try:
                typ, side, data, pts, reduced = self.sample_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            entry = self.sync_buffer[reduced]
            if typ == 'frame':
                key_f = f'frame_{side}'
                key_pf = f'pts_frame_{side}'
                if entry[key_f] is None:
                    entry[key_f] = data
                    entry[key_pf] = pts
            else:
                key_k = f'klv_{side}'
                key_pk = f'pts_klv_{side}'
                if entry[key_k] is None:
                    entry[key_k] = data
                    entry[key_pk] = pts
            e = entry
            if all(e[k] is not None for k in ('frame_left','klv_left','frame_right','klv_right')):
                print(f"Reduced PTS: {reduced:.3f}s")
                print(f"  Left Frame  : {e['pts_frame_left']:.3f}s, file={e['klv_left']}")
                print(f"  Right Frame : {e['pts_frame_right']:.3f}s, file={e['klv_right']}")
                # Concatenate and save
                left = e['frame_left']
                right = e['frame_right']
                if left is not None and right is not None:
                    concat = np.hstack((left, right))
                    img_name = f"{reduced:.3f}_L_{e['klv_left']}_R_{e['klv_right']}.png".replace('/', '_')
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
        if hasattr(self, 'meta_pipe'):
            self.meta_pipe.set_state(Gst.State.NULL)
        for p in getattr(self, 'video_pipes', {}).values():
            p.set_state(Gst.State.NULL)
        self.running = False

    def run(self):
        print(f"Streaming LEFT {SRC_URI_LEFT}, RIGHT {SRC_URI_RIGHT}, KLV tcp://{TCP_HOST}:{TCP_PORT}")
        try:
            self.loop.run()
        except KeyboardInterrupt:
            pass
        finally:
            self._cleanup()

if __name__ == '__main__':
    app = SRTSyncClient(fps=8)
    app.run()
