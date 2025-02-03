import cv2
import numpy as np
from typing import Dict, Tuple
import logging

logger = logging.getLogger(__name__)

class DocumentPreprocessor:
    def __init__(self):
        self.debug = True

    def preprocess_emirates_id_front(self, image: np.ndarray) -> np.ndarray:
        """Enhanced preprocessing for Emirates ID front."""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Remove background patterns
            denoised = cv2.fastNlMeansDenoising(gray)
            
            # Enhance contrast
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            enhanced = clahe.apply(denoised)
            
            # Adaptive thresholding
            binary = cv2.adaptiveThreshold(
                enhanced, 255, 
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY, 11, 2
            )
            
            # Remove noise
            kernel = cv2.getStructuringElement(cv2.MORPH_RECT, (3,3))
            cleaned = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
            
            return cleaned

        except Exception as e:
            logger.error(f"Error preprocessing Emirates ID front: {str(e)}")
            return image

    def preprocess_emirates_id_back(self, image: np.ndarray) -> np.ndarray:
        """Enhanced preprocessing for Emirates ID back."""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Enhanced denoising for back side
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            
            # Sharpen the image
            kernel = np.array([[-1,-1,-1], [-1,9,-1], [-1,-1,-1]])
            sharpened = cv2.filter2D(denoised, -1, kernel)
            
            # Binarization with Otsu's method
            _, binary = cv2.threshold(sharpened, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            
            return binary

        except Exception as e:
            logger.error(f"Error preprocessing Emirates ID back: {str(e)}")
            return image

    def preprocess_passport(self, image: np.ndarray) -> np.ndarray:
        """Enhanced preprocessing for passport."""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Remove glare and enhance contrast
            clahe = cv2.createCLAHE(clipLimit=3.0, tileGridSize=(8,8))
            enhanced = clahe.apply(gray)
            
            # Bilateral filtering to preserve edges
            denoised = cv2.bilateralFilter(enhanced, 9, 75, 75)
            
            # Otsu's thresholding with additional processing
            _, binary = cv2.threshold(denoised, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
            
            # Clean up using morphological operations
            kernel = np.ones((2,2), np.uint8)
            cleaned = cv2.morphologyEx(binary, cv2.MORPH_CLOSE, kernel)
            
            return cleaned

        except Exception as e:
            logger.error(f"Error preprocessing passport: {str(e)}")
            return image

    def preprocess_visa(self, image: np.ndarray) -> np.ndarray:
        """Enhanced preprocessing for visa document."""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Enhanced denoising
            denoised = cv2.fastNlMeansDenoising(gray, None, 10, 7, 21)
            
            # Enhance contrast
            clahe = cv2.createCLAHE(clipLimit=2.0, tileGridSize=(8,8))
            enhanced = clahe.apply(denoised)
            
            # Adaptive thresholding
            binary = cv2.adaptiveThreshold(
                enhanced, 255,
                cv2.ADAPTIVE_THRESH_GAUSSIAN_C,
                cv2.THRESH_BINARY, 11, 2
            )
            
            return binary

        except Exception as e:
            logger.error(f"Error preprocessing visa: {str(e)}")
            return image

    def detect_document_skew(self, image: np.ndarray) -> float:
        """Detect and return document skew angle."""
        try:
            # Convert to grayscale
            gray = cv2.cvtColor(image, cv2.COLOR_BGR2GRAY)
            
            # Detect edges
            edges = cv2.Canny(gray, 50, 150, apertureSize=3)
            
            # Use Hough transform to detect lines
            lines = cv2.HoughLines(edges, 1, np.pi/180, 200)
            
            if lines is not None:
                angles = []
                for rho, theta in lines[0]:
                    angle = np.degrees(theta)
                    if angle < 45:
                        angles.append(angle)
                    elif angle > 135:
                        angles.append(angle - 180)
                
                if angles:
                    return np.mean(angles)
            
            return 0.0

        except Exception as e:
            logger.error(f"Error detecting document skew: {str(e)}")
            return 0.0

    def correct_skew(self, image: np.ndarray) -> np.ndarray:
        """Correct document skew."""
        try:
            angle = self.detect_document_skew(image)
            if abs(angle) > 0.5:  # Only correct if skew is significant
                (h, w) = image.shape[:2]
                center = (w // 2, h // 2)
                M = cv2.getRotationMatrix2D(center, angle, 1.0)
                rotated = cv2.warpAffine(image, M, (w, h),
                                       flags=cv2.INTER_CUBIC,
                                       borderMode=cv2.BORDER_REPLICATE)
                return rotated
            
            return image

        except Exception as e:
            logger.error(f"Error correcting skew: {str(e)}")
            return image

    def get_preprocessed_image(self, image: np.ndarray, doc_type: str) -> np.ndarray:
        """Get preprocessed image based on document type."""
        try:
            # First correct any skew
            deskewed = self.correct_skew(image)
            
            # Apply document-specific preprocessing
            if doc_type == 'emirates_id_front':
                return self.preprocess_emirates_id_front(deskewed)
            elif doc_type == 'emirates_id_back':
                return self.preprocess_emirates_id_back(deskewed)
            elif doc_type == 'passport':
                return self.preprocess_passport(deskewed)
            elif doc_type == 'uae_visa':
                return self.preprocess_visa(deskewed)
            else:
                logger.warning(f"Unknown document type: {doc_type}")
                return deskewed

        except Exception as e:
            logger.error(f"Error in preprocessing: {str(e)}")
            return image