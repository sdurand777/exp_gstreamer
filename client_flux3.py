
#!/usr/bin/env python3
import struct
import gi
from collections import defaultdict
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
TCP_HOST = "127.0.0.1"
TCP_PORT = 7000
SRC_URI_LEFT = "srt://127.0.0.1:6020?mode=caller"
SRC_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"

class SRTSyncPrinter:
    def __init__(self, fps=8):
        Gst.init(None)
        self.loop = GLib.MainLoop()
        self.precision = 1.0 / fps
        # Aligners for PTS reduction
        self.aligners = {side: self._make_aligner() for side in ('left','right')}
        # Buffer reduced_pts -> data dict
        self.sync_buffer = defaultdict(lambda: {
            'frame_left': None, 'pts_frame_left': None,
            'klv_left': None,  'pts_klv_left': None,
            'frame_right': None,'pts_frame_right': None,
            'klv_right': None, 'pts_klv_right': None
        })
        # Keep pipeline refs for cleanup
        self.meta_pipe = None
        self.video_pipes = {}
        # Build and launch pipelines
        self._build_meta_pipeline()
        self._build_video_pipeline('left', SRC_URI_LEFT)
        self._build_video_pipeline('right', SRC_URI_RIGHT)

    def _make_aligner(self):
        state = {'offset': None}
        def align(pts):
            if pts < 0:
                return -1
            if state['offset'] is None:
                base = int(pts/self.precision) * self.precision
                state['offset'] = pts - base
            aligned = pts - state['offset']
            return round(aligned/self.precision) * self.precision
        return align

    def _parse_klv_filename(self, buf):
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
        except:
            return None

    def _build_meta_pipeline(self):
        desc = (
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=demux "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_left emit-signals=true sync=false drop=true "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_right emit-signals=true sync=false drop=true"
        )
        self.meta_pipe = Gst.parse_launch(desc)
        # Connect KLV handlers
        self.meta_pipe.get_by_name('klv_left') .connect('new-sample', self._make_klv_handler('left'))
        self.meta_pipe.get_by_name('klv_right').connect('new-sample', self._make_klv_handler('right'))
        # Bus watch
        bus = self.meta_pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        # Start
        self.meta_pipe.set_state(Gst.State.PLAYING)

    def _make_klv_handler(self, side):
        def on_sample(sink):
            sample = sink.emit('pull-sample')
            buf = sample.get_buffer()
            pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
            fname = self._parse_klv_filename(buf)
            if fname:
                reduced = self.aligners[side](pts)
                entry = self.sync_buffer[reduced]
                entry[f'klv_{side}'] = fname
                entry[f'pts_klv_{side}'] = pts
                self._try_print(reduced)
            return Gst.FlowReturn.OK
        return on_sample

    def _build_video_pipeline(self, side, uri):
        desc = (
            f"srtsrc latency=200 uri={uri} ! queue ! application/x-rtp,media=video,encoding-name=JPEG,payload=26 "
            "! rtpjpegdepay ! nvjpegdec ! videoconvert ! video/x-raw,format=RGB ! "
            f"appsink name=vid_{side} emit-signals=true sync=true max-buffers=1 drop=true"
        )
        pipe = Gst.parse_launch(desc)
        # Connect video handler
        pipe.get_by_name(f'vid_{side}').connect('new-sample', self._make_video_handler(side))
        # Bus watch
        bus = pipe.get_bus()
        bus.add_signal_watch()
        bus.connect('message', self._on_message)
        # Start
        pipe.set_state(Gst.State.PLAYING)
        # Save ref
        self.video_pipes[side] = pipe

    def _make_video_handler(self, side):
        def on_sample(sink):
            sample = sink.emit('pull-sample')
            buf = sample.get_buffer()
            pts = buf.pts / Gst.SECOND if buf.pts != Gst.CLOCK_TIME_NONE else -1
            ok, info = buf.map(Gst.MapFlags.READ)
            if ok:
                buf.unmap(info)
                reduced = self.aligners[side](pts)
                entry = self.sync_buffer[reduced]
                entry[f'frame_{side}'] = True
                entry[f'pts_frame_{side}'] = pts
                self._try_print(reduced)
            return Gst.FlowReturn.OK
        return on_sample

    def _try_print(self, reduced):
        entry = self.sync_buffer[reduced]
        if all([entry['frame_left'], entry['klv_left'], entry['frame_right'], entry['klv_right']]):
            print(f"Reduced PTS: {reduced:.3f}s")
            print(f"  Left Frame    : {entry['pts_frame_left']:.3f}s")
            print(f"  Left KLV      : {entry['pts_klv_left']:.3f}s")
            print(f"  Right Frame   : {entry['pts_frame_right']:.3f}s")
            print(f"  Right KLV     : {entry['pts_klv_right']:.3f}s")
            print("-----")
            # Clean up
            del self.sync_buffer[reduced]

    def _on_message(self, bus, msg):
        if msg.type == Gst.MessageType.ERROR:
            err,dbg = msg.parse_error()
            print("[ERROR]", err.message)
            # Stop pipelines cleanly
            self._cleanup()
            self.loop.quit()
        elif msg.type == Gst.MessageType.EOS:
            self._cleanup()
            self.loop.quit()

    def _cleanup(self):
        # Set all pipelines to NULL state
        if self.meta_pipe:
            self.meta_pipe.set_state(Gst.State.NULL)
        for pipe in self.video_pipes.values():
            pipe.set_state(Gst.State.NULL)

    def run(self):
        print(f"Streaming LEFT {SRC_URI_LEFT}, RIGHT {SRC_URI_RIGHT}, KLV tcp://{TCP_HOST}:{TCP_PORT}")
        try:
            self.loop.run()
        except KeyboardInterrupt:
            pass
        finally:
            self._cleanup()

if __name__ == '__main__':
    app = SRTSyncPrinter(fps=8)
    app.run()
