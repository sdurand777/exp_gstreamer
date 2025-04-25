
#!/usr/bin/env python3
import struct
import gi
import threading
import queue
import collections
import os
import numpy as np
import cv2
from collections import defaultdict
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
TCP_HOST     = "127.0.0.1"
TCP_PORT     = 7000
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
            'frame_left':   None, 'pts_frame_left':   None,
            'klv_left':     None, 'pts_klv_left':     None,
            'frame_right':  None, 'pts_frame_right':  None,
            'klv_right':    None, 'pts_klv_right':    None
        })

        # PTS sync variables (global offset, precision)
        self.global_offset = None
        self.pts_precision = 1.0 / fps

        # Start processing thread
        self.running = True
        self.processor_thread = threading.Thread(target=self._process_samples, daemon=True)
        self.processor_thread.start()

        # Build and launch pipelines
        self.meta_pipe = None
        self.video_pipes = {}
        self._build_meta_pipeline()
        self._build_video_pipeline('left',  SRC_URI_LEFT)
        self._build_video_pipeline('right', SRC_URI_RIGHT)

    def _build_meta_pipeline(self):
        desc = (
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=demux "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_left "
            "emit-signals=true sync=false drop=true "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_right "
            "emit-signals=true sync=true max-buffers=1 drop=true"
        )
        self.meta_pipe = Gst.parse_launch(desc)
        self.meta_pipe.get_by_name('klv_left').connect('new-sample', self._on_meta('left'))
        self.meta_pipe.get_by_name('klv_right').connect('new-sample', self._on_meta('right'))
        bus = self.meta_pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        self.meta_pipe.set_state(Gst.State.PLAYING)

    def _build_video_pipeline(self, side, uri):
        desc = (
            f"srtsrc latency=200 uri={uri} ! queue ! "
            "application/x-rtp,media=video,encoding-name=JPEG,payload=26 ! rtpjpegdepay ! nvjpegdec ! "
            "videoconvert ! video/x-raw,format=RGB ! "
            f"appsink name=vid_{side} emit-signals=true sync=true max-buffers=1 drop=true"
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
            pts   = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1

            fname = self._parse_klv(buf)

            if side is "left":
                print("############### pts left meta = ", pts, " fname ", fname)
            else:
                print("############### pts right meta = ", pts, "fname ", fname)

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

            if side is "left":
                print("============= pts left video = ", pts)
            else:
                print("============= pts right video = ", pts)

            reduced = self._calc_reduced_pts(pts)
            # Extract raw frame data
            ok, info = buf.map(Gst.MapFlags.READ)
            frame_img = None
            if ok:
                data = info.data
                buf.unmap(info)
                structure = sample.get_caps().get_structure(0)
                width  = structure.get_value('width')
                height = structure.get_value('height')
                arr = np.ndarray((height, width, 3), dtype=np.uint8, buffer=data)
                frame_img = cv2.cvtColor(arr, cv2.COLOR_RGB2BGR)
            self.sample_queue.put(('frame', side, frame_img, pts, reduced))
            return Gst.FlowReturn.OK
        return handler

    def _parse_klv(self, buf):
        ok, info = buf.map(Gst.MapFlags.READ)
        if not ok: return None
        data = info.data
        buf.unmap(info)
        if len(data) < 20: return None
        length = struct.unpack('>I', data[16:20])[0]
        payload = data[20:20+length]
        msg = info_pb2.StreamInfo()
        try:
            msg.ParseFromString(payload)
            return msg.filename
        except:
            return None

    def _calc_reduced_pts(self, pts_time):
        """
        Calculate and align PTS using a global offset, updated with a running average.
        """
        if pts_time < 0:
            return -1
        if self.global_offset is None:
            down = int(pts_time / self.pts_precision) * self.pts_precision
            self.global_offset = pts_time - down
        else:
            down = round((pts_time - self.global_offset) / self.pts_precision) * self.pts_precision
            new_offset = pts_time - down
            current_offset = self.global_offset
            updated_offset = 0.8 * current_offset + 0.2 * new_offset
            if abs(updated_offset - current_offset) > 0.01:
                self.global_offset = updated_offset
                print(f"Offset updated: old={current_offset:.6f}s new={updated_offset:.6f}s")
        aligned = pts_time - self.global_offset
        reduced = round(aligned / self.pts_precision) * self.pts_precision
        return reduced

    # def _process_samples(self):
    #     while self.running:
    #         try:
    #             typ, side, data, pts, reduced = self.sample_queue.get(timeout=0.1)
    #         except queue.Empty:
    #             continue
    #         entry = self.sync_buffer[reduced]
    #         if typ == 'frame':
    #             entry[f'frame_{side}']      = data
    #             entry[f'pts_frame_{side}']   = pts
    #         else:
    #             entry[f'klv_{side}']        = data
    #             entry[f'pts_klv_{side}']     = pts
    #         e = entry
    #         if all(e[k] is not None for k in (
    #             'frame_left','klv_left','frame_right','klv_right'
    #         )):
    #             print(f"Reduced PTS: {reduced:.3f}s")
    #             print(f"  Left Frame    : {e['pts_frame_left']:.3f}s")
    #             print(f"  Left KLV      : {e['pts_klv_left']:.3f}s, filename={e['klv_left']}")
    #             print(f"  Right Frame   : {e['pts_frame_right']:.3f}s")
    #             print(f"  Right KLV     : {e['pts_klv_right']:.3f}s, filename={e['klv_right']}")
    #             print("-----")
    #             left = e['frame_left']
    #             right= e['frame_right']
    #             if left is not None and right is not None:
    #                 concat = np.hstack((left, right))
    #                 img_name = f"{reduced:.3f}_L_{e['klv_left']}_R_{e['klv_right']}.png".replace('/', '_')
    #                 path = os.path.join('save', img_name)
    #                 cv2.imwrite(path, concat)
    #             del self.sync_buffer[reduced]

    def _process_samples(self):
        while self.running:
            try:
                typ, side, data, pts, reduced = self.sample_queue.get(timeout=0.1)
            except queue.Empty:
                continue
            entry = self.sync_buffer[reduced]
            if typ == 'frame':
                key_frame = f'frame_{side}'
                key_pts   = f'pts_frame_{side}'
                # only record first frame per side per bucket
                if entry[key_frame] is None:
                    entry[key_frame] = data
                    entry[key_pts]   = pts
            else:
                key_klv = f'klv_{side}'
                key_pts_klv = f'pts_klv_{side}'
                # only record first KLV per side per bucket
                if entry[key_klv] is None:
                    entry[key_klv]     = data
                    entry[key_pts_klv] = pts

                # entry[f'klv_{side}']        = data
                # entry[f'pts_klv_{side}']     = pts
            e = entry
            if all(e[k] is not None for k in (
                'frame_left','klv_left','frame_right','klv_right'
            )):
                print(f"Reduced PTS: {reduced:.3f}s")
                print(f"  Left Frame    : {e['pts_frame_left']:.3f}s")
                print(f"  Left KLV      : {e['pts_klv_left']:.3f}s, filename={e['klv_left']}")
                print(f"  Right Frame   : {e['pts_frame_right']:.3f}s")
                print(f"  Right KLV     : {e['pts_klv_right']:.3f}s, filename={e['klv_right']}")
                print("-----")
                left = e['frame_left']
                right= e['frame_right']
                if left is not None and right is not None:
                    concat = np.hstack((left, right))
                    img_name = f"{reduced:.3f}_L_{e['klv_left']}_R_{e['klv_right']}.png".replace('/', '_')
                    path = os.path.join('save', img_name)
                    cv2.imwrite(path, concat)
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
        if self.meta_pipe:
            self.meta_pipe.set_state(Gst.State.NULL)
        for p in self.video_pipes.values():
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
