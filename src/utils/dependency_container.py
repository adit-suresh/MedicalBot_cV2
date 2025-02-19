from typing import Dict, Any, Type, Optional, Callable, Set
import inspect
import logging
import threading

logger = logging.getLogger(__name__)

class DependencyContainer:
    """Container for managing dependencies with improved type safety and error handling."""
    
    def __init__(self):
        self._services: Dict[str, Type] = {}
        self._factories: Dict[str, Callable] = {}
        self._instances: Dict[str, Any] = {}
        self._resolving: Set[str] = set()  # Track dependencies being resolved to detect circular dependencies
        self._lock = threading.RLock()  # Thread-safe resolution

    def register(self, interface: Type, implementation: Type) -> None:
        """Register a service implementation with improved type safety.
        
        Args:
            interface: Interface or base class
            implementation: Concrete implementation class
            
        Raises:
            ValueError: If implementation doesn't inherit from interface
        """
        with self._lock:
            try:
                if not issubclass(implementation, interface):
                    raise ValueError(f"{implementation.__name__} is not a subclass of {interface.__name__}")
                self._services[interface.__name__] = implementation
                logger.debug(f"Registered {implementation.__name__} for {interface.__name__}")
            except TypeError:
                # Handle case where interface or implementation aren't classes
                raise ValueError(f"Both interface and implementation must be classes")

    def register_instance(self, interface: Type, instance: Any) -> None:
        """Register a pre-configured instance.
        
        Args:
            interface: Interface or base class
            instance: Instance to register
            
        Raises:
            ValueError: If instance is not of the expected type
        """
        with self._lock:
            if not isinstance(instance, interface):
                raise ValueError(f"Instance is not of type {interface.__name__}")
            self._instances[interface.__name__] = instance
            logger.debug(f"Registered instance for {interface.__name__}")

    def register_factory(self, interface: Type, factory: Callable[[], Any]) -> None:
        """Register a factory function for creating instances.
        
        Args:
            interface: Interface or base class
            factory: Factory function that returns an instance
        """
        with self._lock:
            self._factories[interface.__name__] = factory
            logger.debug(f"Registered factory for {interface.__name__}")

    def resolve(self, interface: Type) -> Any:
        """Resolve and return an instance of the requested interface.
        
        Args:
            interface: Interface to resolve
            
        Returns:
            Instance of the requested interface
            
        Raises:
            ValueError: If interface is not registered or circular dependency detected
        """
        with self._lock:
            name = interface.__name__
            
            # Detect circular dependencies
            if name in self._resolving:
                chain = " -> ".join(self._resolving) + f" -> {name}"
                raise ValueError(f"Circular dependency detected: {chain}")
            
            # Return existing instance if available
            if name in self._instances:
                return self._instances[name]
                
            # Use factory if registered
            if name in self._factories:
                self._resolving.add(name)
                try:
                    instance = self._factories[name]()
                    self._instances[name] = instance
                    return instance
                finally:
                    self._resolving.remove(name)
            
            # Create new instance if service is registered
            if name in self._services:
                implementation = self._services[name]
                self._resolving.add(name)
                try:
                    instance = self._create_instance(implementation)
                    self._instances[name] = instance
                    return instance
                finally:
                    self._resolving.remove(name)
            
            available_services = list(self._services.keys()) + list(self._instances.keys()) + list(self._factories.keys())
            raise ValueError(f"No registration found for {name}. Available services: {available_services}")

    def _create_instance(self, cls: Type) -> Any:
        """Create an instance of a class, resolving its dependencies.
        
        Args:
            cls: Class to instantiate
            
        Returns:
            Instance of the class
            
        Raises:
            ValueError: If dependencies cannot be resolved
        """
        signature = inspect.signature(cls.__init__)
        parameters = {}

        for param_name, param in signature.parameters.items():
            if param_name == 'self':
                continue

            # Try to resolve parameter type
            if param.annotation != inspect.Parameter.empty:
                try:
                    parameters[param_name] = self.resolve(param.annotation)
                except ValueError as e:
                    if param.default != inspect.Parameter.empty:
                        parameters[param_name] = param.default
                    else:
                        raise ValueError(f"Cannot resolve parameter {param_name} of type {param.annotation}: {str(e)}")
            elif param.default != inspect.Parameter.empty:
                # Use default value if available
                parameters[param_name] = param.default

        return cls(**parameters)

    def clear(self) -> None:
        """Clear all registrations (useful for testing)."""
        with self._lock:
            self._services.clear()
            self._factories.clear()
            self._instances.clear()


# Global container instance
container = DependencyContainer()

def inject(*interfaces: Type) -> Callable:
    """Decorator for injecting dependencies.
    
    Args:
        *interfaces: Interfaces to inject
        
    Returns:
        Decorator function that injects dependencies into class initialization
    """
    def decorator(cls: Type) -> Type:
        original_init = cls.__init__

        def new_init(self: Any, *args: Any, **kwargs: Any) -> None:
            # Inject dependencies
            for interface in interfaces:
                try:
                    setattr(self, f"_{interface.__name__.lower()}", container.resolve(interface))
                except ValueError as e:
                    logger.error(f"Failed to inject {interface.__name__} into {cls.__name__}: {str(e)}")
                    raise
            
            # Call original init
            original_init(self, *args, **kwargs)

        cls.__init__ = new_init
        return cls

    return decorator