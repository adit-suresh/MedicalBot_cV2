import requests
import logging
from typing import Dict, Optional
from datetime import datetime, timedelta
import time

logger = logging.getLogger(__name__)

class ServiceStatus:
    OK = "ok"
    DOWN = "down"
    DEGRADED = "degraded"

class ServiceMonitor:
    def __init__(self):
        self.services = {
            "nas_portal": {
                "url": "https://nas-portal-url.com/health",  # Replace with actual URL
                "timeout": 10,
                "retry_count": 3,
                "retry_delay": 5,  # seconds
                "last_check": None,
                "status": None
            },
            "google_vision": {
                "test_method": self._test_google_vision,
                "retry_count": 2,
                "retry_delay": 3,
                "last_check": None,
                "status": None
            }
            # Add other services as needed
        }

    def check_service(self, service_name: str) -> Dict:
        """Check if a service is available."""
        if service_name not in self.services:
            raise ValueError(f"Unknown service: {service_name}")

        service = self.services[service_name]
        retry_count = service["retry_count"]
        retry_delay = service["retry_delay"]

        for attempt in range(retry_count):
            try:
                if "url" in service:
                    # HTTP service check
                    response = requests.get(
                        service["url"], 
                        timeout=service["timeout"]
                    )
                    if response.status_code == 200:
                        status = ServiceStatus.OK
                    else:
                        status = ServiceStatus.DEGRADED
                elif "test_method" in service:
                    # Custom test method
                    status = service["test_method"]()
                else:
                    raise ValueError(f"No test method for service: {service_name}")

                service["last_check"] = datetime.now()
                service["status"] = status
                
                return {
                    "status": status,
                    "last_check": service["last_check"],
                    "attempts": attempt + 1
                }

            except requests.exceptions.RequestException as e:
                logger.warning(
                    f"Service {service_name} check failed (attempt {attempt + 1}): {str(e)}"
                )
                if attempt < retry_count - 1:time.sleep(retry_delay)
                continue

        # All retries failed
        status = ServiceStatus.DOWN
        service["last_check"] = datetime.now()
        service["status"] = status
        
        return {
            "status": status,
            "last_check": service["last_check"],
            "attempts": retry_count
        }

    def _test_google_vision(self) -> str:
        """Test Google Vision API availability."""
        try:
            from google.cloud import vision
            client = vision.ImageAnnotatorClient()
            
            # Create a simple test image
            from PIL import Image
            import io
            img = Image.new('RGB', (60, 30), color='white')
            img_byte_arr = io.BytesIO()
            img.save(img_byte_arr, format='PNG')
            img_byte_arr = img_byte_arr.getvalue()

            # Try to use the API
            image = vision.Image(content=img_byte_arr)
            response = client.text_detection(image=image)
            
            if response.error.message:
                return ServiceStatus.DEGRADED
            return ServiceStatus.OK

        except Exception as e:
            logger.error(f"Google Vision API test failed: {str(e)}")
            return ServiceStatus.DOWN

    def check_all_services(self) -> Dict[str, Dict]:
        """Check status of all services."""
        results = {}
        for service_name in self.services:
            results[service_name] = self.check_service(service_name)
        return results

    def is_service_available(self, service_name: str, max_age: int = 300) -> bool:
        """
        Check if service is available using cached status if recent.
        
        Args:
            service_name: Name of the service to check
            max_age: Maximum age of cached status in seconds
        """
        service = self.services.get(service_name)
        if not service:
            return False

        now = datetime.now()
        last_check = service.get("last_check")
        
        # If status is recent enough, use cached value
        if (last_check and 
            (now - last_check).total_seconds() < max_age and 
            service.get("status") == ServiceStatus.OK):
            return True

        # Otherwise do a fresh check
        result = self.check_service(service_name)
        return result["status"] == ServiceStatus.OK

    def wait_for_service(self, service_name: str, timeout: int = 300, 
                        check_interval: int = 10) -> bool:
        """
        Wait for a service to become available.
        
        Args:
            service_name: Name of the service to wait for
            timeout: Maximum time to wait in seconds
            check_interval: Time between checks in seconds
        
        Returns:
            bool: True if service became available, False if timeout reached
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            if self.is_service_available(service_name):
                return True
            time.sleep(check_interval)
        
        return False