import requests
import logging
from datetime import datetime, timedelta
import time
from typing import Dict, Optional
from enum import Enum

logger = logging.getLogger(__name__)

class PortalStatus(Enum):
    UP = "up"
    DOWN = "down"
    DEGRADED = "degraded"
    UNKNOWN = "unknown"

class PortalChecker:
    def __init__(self):
        self.portal_url = "https://insurance-portal-url.com"  # Replace with actual URL
        self.health_endpoint = "/health"
        self.last_check = None
        self.last_status = None
        self.check_interval = 300  # 5 minutes
        self.max_retries = 3
        self.retry_delay = 5  # seconds

    def check_status(self, force: bool = False) -> PortalStatus:
        """
        Check portal status with caching.
        
        Args:
            force: Force fresh check ignoring cache
            
        Returns:
            PortalStatus enum
        """
        # Use cached status if recent enough
        if not force and self.last_check and self.last_status:
            age = (datetime.now() - self.last_check).total_seconds()
            if age < self.check_interval:
                return self.last_status

        status = self._perform_check()
        self.last_check = datetime.now()
        self.last_status = status
        return status

    def _perform_check(self) -> PortalStatus:
        """Perform actual status check."""
        for attempt in range(self.max_retries):
            try:
                response = requests.get(
                    f"{self.portal_url}{self.health_endpoint}",
                    timeout=10
                )
                
                if response.status_code == 200:
                    return PortalStatus.UP
                elif response.status_code >= 500:
                    return PortalStatus.DOWN
                else:
                    return PortalStatus.DEGRADED

            except requests.RequestException:
                if attempt < self.max_retries - 1:
                    time.sleep(self.retry_delay)
                    continue
                else:
                    return PortalStatus.DOWN

        return PortalStatus.UNKNOWN

    def wait_for_portal(self, 
                       timeout: int = 1800,  # 30 minutes 
                       check_interval: int = 60) -> bool:
        """
        Wait for portal to become available.
        
        Args:
            timeout: Maximum time to wait in seconds
            check_interval: Time between checks in seconds
            
        Returns:
            bool: True if portal becomes available, False if timeout reached
        """
        start_time = datetime.now()
        
        while (datetime.now() - start_time).total_seconds() < timeout:
            status = self.check_status(force=True)
            if status == PortalStatus.UP:
                return True
            time.sleep(check_interval)
        
        return False

    def verify_login(self) -> bool:
        """Verify if portal login is working."""
        try:
            # Implement login check using your portal's authentication mechanism
            # This is a placeholder - implement actual login verification
            return True
        except Exception as e:
            logger.error(f"Login verification failed: {str(e)}")
            return False

    def get_detailed_status(self) -> Dict:
        """Get detailed portal status information."""
        status = self.check_status()
        
        return {
            'status': status.value,
            'last_check': self.last_check.isoformat() if self.last_check else None,
            'response_time': self._check_response_time(),
            'login_working': self.verify_login() if status == PortalStatus.UP else False
        }

    def _check_response_time(self) -> Optional[float]:
        """Check portal response time."""
        try:
            start_time = time.time()
            requests.get(self.portal_url, timeout=10)
            return time.time() - start_time
        except:
            return None