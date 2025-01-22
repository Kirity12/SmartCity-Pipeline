import cv2
import uuid
import base64
import numpy as np
from io import BytesIO
from PIL import Image

def video_duration(video_path):
    # Open the video file to get its total frames and FPS
    video = cv2.VideoCapture(video_path)
    total_frames = int(video.get(cv2.CAP_PROP_FRAME_COUNT))
    fps = video.get(cv2.CAP_PROP_FPS)
    duration = total_frames / fps
    video.release()
    return duration

def encode_image_to_base64(image_path):
    """
    Converts a given image file to a Base64 encoded string.
    """
    with open(image_path, "rb") as image_file:
        encoded_image = base64.b64encode(image_file.read()).decode('utf-8')
    return encoded_image

def extract_frame_at_timestamp(timestamp):
    """
    Extracts a frame from the video at the given timestamp (in seconds).
    """

    try:
        # Open the video
        video = cv2.VideoCapture(r'C:\Users\Kirity\Downloads\CarDrive.mp4')
        
        # Get video properties
        fps = video.get(cv2.CAP_PROP_FPS)
        
        # Calculate the corresponding frame number
        frame_number = int(timestamp * fps)
        
        # Set the video to the desired frame number
        video.set(cv2.CAP_PROP_POS_FRAMES, frame_number)
        
        # Read the frame at that position
        ret, frame = video.read()
        
        if ret:
            # Convert the frame from BGR (OpenCV default) to RGB
            frame_rgb = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
            
            # Convert the frame to a PIL image for easy manipulation
            pil_image = Image.fromarray(frame_rgb)
            
            # Save the image to a buffer in JPEG format
            buffer = BytesIO()
            pil_image.save(buffer, format="JPEG")
            buffer.seek(0)
            
            # Convert the image to Base64
            base64_image = base64.b64encode(buffer.read()).decode('utf-8')
            
            # Explicitly close the buffer to release memory
            buffer.close()
                    
            video.release()
            return base64_image
        else:
            print(f"Error: {e}. Returning black image as fallback.")
            black_image_base64 = encode_image_to_base64(r'C:\Users\Kirity\Downloads\imageNotFound.png')
            
            return black_image_base64
    except Exception as e:
        # If an error occurs, return a base64-encoded black image
        print(f"Error: {e}. Returning black image as fallback.")
        black_image_base64 = encode_image_to_base64(r'C:\Users\Kirity\Downloads\imageNotFound.png')
        
        return black_image_base64