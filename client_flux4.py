
#!/usr/bin/env python3
import struct
import gi
import cv2
import numpy as np
import threading
from collections import defaultdict
import info_pb2

gi.require_version('Gst', '1.0')
from gi.repository import Gst, GLib

# --- Configuration ---
TCP_HOST = "127.0.0.1"
TCP_PORT = 7000
SRC_URI_LEFT = "srt://127.0.0.1:6020?mode=caller"
SRC_URI_RIGHT = "srt://127.0.0.1:6021?mode=caller"
FPS = 8

class SRTSyncDisplay:
    def __init__(self, fps=FPS):
        Gst.init(None)
        cv2.namedWindow('SyncView', cv2.WINDOW_AUTOSIZE)
        self.loop = GLib.MainLoop()
        self.precision = 1.0 / fps
        self.lock = threading.Lock()
        # per-side aligners
        self.aligners = {side: self._make_aligner() for side in ('left','right')}
        # intermediate sync by PTS: reduced -> entry
        self.pts_buffer = defaultdict(lambda: {
            'frame_left': None, 'pts_frame_left': None,
            'klv_left_img': None, 'klv_left_fname': None, 'pts_klv_left': None,
            'frame_right': None,'pts_frame_right': None,
            'klv_right_img': None,'klv_right_fname': None,'pts_klv_right': None
        })
        # final matches by filename
        self.left_matches = {}
        self.right_matches = {}
        # build pipelines
        self._build_meta_pipeline()
        self._build_video_pipeline('left', SRC_URI_LEFT)
        self._build_video_pipeline('right', SRC_URI_RIGHT)

    def _make_aligner(self):
        state = {'offset': None}
        def align(pts):
            if pts < 0:
                return -1
            if state['offset'] is None:
                base = int(pts/self.precision)*self.precision
                state['offset'] = pts - base
            aligned = pts - state['offset']
            return round(aligned/self.precision)*self.precision
        return align

    def _parse_klv(self, buf):
        ok, info = buf.map(Gst.MapFlags.READ)
        if not ok: return None
        data = info.data; buf.unmap(info)
        if len(data) < 20: return None
        length = struct.unpack('>I', data[16:20])[0]
        payload = data[20:20+length]
        msg = info_pb2.StreamInfo()
        try: msg.ParseFromString(payload); return msg.filename
        except: return None

    def _build_meta_pipeline(self):
        desc = (
            f"tcpclientsrc host={TCP_HOST} port={TCP_PORT} ! tsdemux name=demux "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_left emit-signals=true sync=false drop=true "
            "demux. ! queue ! meta/x-klv,parsed=true ! appsink name=klv_right emit-signals=true sync=false drop=true"
        )
        pipe = Gst.parse_launch(desc)
        pipe.get_by_name('klv_left').connect('new-sample', self._make_klv_handler('left'))
        pipe.get_by_name('klv_right').connect('new-sample', self._make_klv_handler('right'))
        bus=pipe.get_bus(); bus.add_signal_watch(); bus.connect('message', self._on_message)
        self.meta_pipe=pipe; pipe.set_state(Gst.State.PLAYING)

    def _make_klv_handler(self, side):
        def on_sample(sink):
            sample=sink.emit('pull-sample'); buf=sample.get_buffer()
            pts=buf.pts/Gst.SECOND if buf.pts!=Gst.CLOCK_TIME_NONE else -1
            fname=self._parse_klv(buf)
            if fname:
                img=cv2.imread(fname)
                reduced=self.aligners[side](pts)
                with self.lock:
                    e=self.pts_buffer[reduced]
                    e[f'klv_{side}_img']=img
                    e[f'klv_{side}_fname']=fname
                    e[f'pts_klv_{side}']=pts
                self._attempt_side_sync(side, reduced)
            return Gst.FlowReturn.OK
        return on_sample

    def _build_video_pipeline(self, side, uri):
        desc=(
            f"srtsrc latency=200 uri={uri} ! queue ! application/x-rtp,media=video,encoding-name=JPEG,payload=26 "
            "! rtpjpegdepay ! nvjpegdec ! videoconvert ! video/x-raw,format=RGB ! "
            f"appsink name=vid_{side} emit-signals=true sync=true max-buffers=1 drop=true"
        )
        pipe=Gst.parse_launch(desc)
        pipe.get_by_name(f'vid_{side}').connect('new-sample', self._make_video_handler(side))
        bus=pipe.get_bus(); bus.add_signal_watch(); bus.connect('message', self._on_message)
        self.video_pipes[side]=pipe; pipe.set_state(Gst.State.PLAYING)

    def _make_video_handler(self, side):
        def on_sample(sink):
            sample=sink.emit('pull-sample'); buf=sample.get_buffer()
            pts=buf.pts/Gst.SECOND if buf.pts!=Gst.CLOCK_TIME_NONE else -1
            ok,info=buf.map(Gst.MapFlags.READ)
            if ok:
                caps=sample.get_caps().get_structure(0)
                w,h=caps.get_value('width'),caps.get_value('height')
                frame=cv2.cvtColor(np.ndarray((h,w,3),np.uint8,info.data),cv2.COLOR_RGB2BGR)
                buf.unmap(info)
                reduced=self.aligners[side](pts)
                with self.lock:
                    e=self.pts_buffer[reduced]
                    e[f'frame_{side}']=frame
                    e[f'pts_frame_{side}']=pts
                self._attempt_side_sync(side, reduced)
            return Gst.FlowReturn.OK
        return on_sample

    def _attempt_side_sync(self, side, reduced):
        with self.lock:
            e=self.pts_buffer.get(reduced)
            if not e: return
            frame_key=f'frame_{side}'; klv_key=f'klv_{side}_img'
            fname_key=f'klv_{side}_fname'
            pts_frame_key=f'pts_frame_{side}'; pts_klv_key=f'pts_klv_{side}'
            if e.get(frame_key) is not None and e.get(klv_key) is not None:
                # side-level match
                frame=e[frame_key]; klv_img=e[klv_key]; fname=e[fname_key]
                pts_f=e[pts_frame_key]; pts_k=e[pts_klv_key]
                buf=(frame,klv_img,pts_f,pts_k)
                # record
                if side=='left': self.left_matches[fname]=buf
                else: self.right_matches[fname]=buf
                # cleanup pts_buffer entry to avoid reuse
                del self.pts_buffer[reduced]
        # after unlocking, attempt cross-side sync by filename
        self._attempt_pair_display(fname)

    def _attempt_pair_display(self, fname):
        with self.lock:
            if fname in self.left_matches and fname in self.right_matches:
                lf,lk,pf_l,pk_l=self.left_matches.pop(fname)
                rf,rk,pf_r,pk_r=self.right_matches.pop(fname)
        # do outside lock
        if 'lf' in locals():
            # print PTS
            print(f"Filename: {fname}")
            print(f"  Left  Frame PTS: {pf_l:.3f}s | KLV PTS: {pk_l:.3f}s")
            print(f"  Right Frame PTS: {pf_r:.3f}s | KLV PTS: {pk_r:.3f}s")
            print("-----")
            # schedule display
            GLib.idle_add(self._display_quad, lf, lk, rf, rk)

    def _display_quad(self, fl, kl, fr, kr):
        h=fl.shape[0]
        def rz(img): return cv2.resize(img,(int(img.shape[1]*h/img.shape[0]),h))
        out=np.hstack([fl,rz(kl),fr,rz(kr)])
        cv2.imshow('SyncView',out);cv2.waitKey(1)
        return False

    def _on_message(self,bus,msg):
        if msg.type==Gst.MessageType.ERROR:
            err,_=msg.parse_error();print('[ERROR]',err.message);self._cleanup();self.loop.quit()
        elif msg.type==Gst.MessageType.EOS:
            self._cleanup();self.loop.quit()

    def _cleanup(self):
        if self.meta_pipe: self.meta_pipe.set_state(Gst.State.NULL)
        for p in self.video_pipes.values(): p.set_state(Gst.State.NULL)

    def run(self):
        print(f"Streaming LEFT {SRC_URI_LEFT}, RIGHT {SRC_URI_RIGHT}, KLV tcp://{TCP_HOST}:{TCP_PORT}")
        self.loop.run();self._cleanup()

if __name__=='__main__':
    SRTSyncDisplay().run()
