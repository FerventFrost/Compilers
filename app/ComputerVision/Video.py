import cv2


class VideoStream:

    def __init__(self, _VideoPath):
        self.VideoPath = _VideoPath
        self.VideoStream = cv2.VideoCapture(self.VideoPath)

    def StopStreaming(self):
        if cv2.waitKey(1) & 0xFF == ord('d'):
            return True
        return False

    def ReleaseVideoStream(self):
        self.VideoStream.release()

    def DisplayFrame(self):
        ret, frame = self.VideoStream.read()
        cv2.imshow("Video", frame)

    def DisplayVideo(self):
        while self.VideoStream.isOpened():
            self.DisplayFrame()
            if self.StopStreaming():
                break

        self.ReleaseVideoStream()

    def destroy(self):
        # Closes all the frames
        cv2.destroyAllWindows()      


if __name__ == '__main__':
    o = VideoStream("Bird.mp4")
    o.DisplayVideo()
