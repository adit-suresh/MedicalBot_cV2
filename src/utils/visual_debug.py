import cv2
import numpy as np
from typing import Dict, Tuple
import os

class OCRVisualDebugger:
    def __init__(self, output_dir: str = "debug_output"):
        self.output_dir = output_dir
        os.makedirs(output_dir, exist_ok=True)

    def visualize_processing(self, image: np.ndarray, doc_type: str, 
                           regions: Dict[str, Tuple[Tuple[int, int], Tuple[int, int]]],
                           results: Dict[str, Dict]) -> str:
        """
        Create a visual debug image showing processed regions and results.
        
        Args:
            image: Original image
            doc_type: Type of document
            regions: Dictionary of region coordinates
            results: Extraction results with confidence scores
            
        Returns:
            Path to saved debug image
        """
        # Create a copy of the image for visualization
        debug_image = image.copy()
        
        # Draw regions and add text
        for field, ((x1, y1), (x2, y2)) in regions.items():
            confidence = results.get(field, {}).get('confidence', 0)
            
            # Color based on confidence
            if confidence >= 0.8:
                color = (0, 255, 0)  # Green for high confidence
            elif confidence >= 0.6:
                color = (0, 255, 255)  # Yellow for medium confidence
            else:
                color = (0, 0, 255)  # Red for low confidence
            
            # Draw rectangle around region
            cv2.rectangle(debug_image, (x1, y1), (x2, y2), color, 2)
            
            # Add field name and confidence
            text = f"{field}: {confidence:.2%}"
            cv2.putText(debug_image, text, (x1, y1-5), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, color, 2)
            
            # Add extracted value
            extracted = results.get(field, {}).get('value', '')
            cv2.putText(debug_image, extracted, (x1, y2+20), 
                       cv2.FONT_HERSHEY_SIMPLEX, 0.5, (255, 255, 255), 2)
        
        # Save debug image
        output_path = os.path.join(self.output_dir, f"debug_{doc_type}.jpg")
        cv2.imwrite(output_path, debug_image)
        return output_path

    def visualize_preprocessing(self, original: np.ndarray, processed: np.ndarray,
                              doc_type: str) -> str:
        """
        Create a visual comparison of original and preprocessed images.
        
        Args:
            original: Original image
            processed: Preprocessed image
            doc_type: Type of document
            
        Returns:
            Path to saved comparison image
        """
        # Ensure both images are same size and type
        if len(original.shape) == 3 and len(processed.shape) == 2:
            processed = cv2.cvtColor(processed, cv2.COLOR_GRAY2BGR)
        
        # Create side-by-side comparison
        comparison = np.hstack((original, processed))
        
        # Add labels
        height = comparison.shape[0]
        cv2.putText(comparison, "Original", (10, height-20), 
                   cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
        cv2.putText(comparison, "Processed", (original.shape[1]+10, height-20),
                   cv2.FONT_HERSHEY_SIMPLEX, 1, (255, 255, 255), 2)
        
        # Save comparison
        output_path = os.path.join(self.output_dir, f"preprocess_{doc_type}.jpg")
        cv2.imwrite(output_path, comparison)
        return output_path