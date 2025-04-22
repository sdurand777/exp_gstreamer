cmd=""" -v videotestsrc is-live=true pattern=ball ! 
video/x-raw,format=I420 ! 
jpegenc ! 
jpegparse ! 
rtpjpegpay mtu=1316 ! 
srtserversink uri="srt://127.0.0.1:6020?mode=listener"
"""

gst-launch-1.0 $cmd
