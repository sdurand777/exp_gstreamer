cmd=""" -v srtsrc uri="srt://127.0.0.1:6020?mode=caller" !
application/x-rtp,encoding-name=JPEG ! 
rtpjpegdepay ! 
jpegdec ! 
videoconvert ! 
autovideosink sync=true
"""

gst-launch-1.0 $cmd
