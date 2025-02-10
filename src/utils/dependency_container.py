from typing import Dict, Any, Type, Optional
import inspect

class DependencyContainer:
    """Container for managing dependencies with lazy initialization."""
    
    def __init__(self):
        self._services: Dict[str, Any] = {}
        self._factories: Dict[str, callable] = {}
        self._instances: Dict[str, Any] = {}

    def register(self, interface: Type, implementation: Type) -> None:
        """Register a service implementation.
        
        Args:
            interface: Interface or base class
            implementation: Concrete implementation class
        """
        self._services[interface.__name__] = implementation

    def register_instance(self, interface: Type, instance: Any) -> None:
        """Register a pre-configured instance.
        
        Args:
            interface: Interface or base class
            instance: Instance to register
        """
        self._instances[interface.__name__] = instance

    def register_factory(self, interface: Type, factory: callable) -> None:
        """Register a factory function for creating instances.
        
        Args:
            interface: Interface or base class
            factory: Factory function
        """
        self._factories[interface.__name__] = factory

    def resolve(self, interface: Type) -> Any:
        """Resolve and return an instance of the requested interface.
        
        Args:
            interface: Interface to resolve
            
        Returns:
            Instance of the requested interface
            
        Raises:
            ValueError: If interface is not registered
        """
        name = interface.__name__

        # Return existing instance if available
        if name in self._instances:
            return self._instances[name]

        # Use factory if registered
        if name in self._factories:
            instance = self._factories[name]()
            self._instances[name] = instance
            return instance

        # Create new instance if service is registered
        if name in self._services:
            implementation = self._services[name]
            instance = self._create_instance(implementation)
            self._instances[name] = instance
            return instance

        raise ValueError(f"No registration found for {name}")

    def _create_instance(self, cls: Type) -> Any:
        """Create an instance of a class, resolving its dependencies.
        
        Args:
            cls: Class to instantiate
            
        Returns:
            Instance of the class
        """
        # Get constructor parameters
        signature = inspect.signature(cls.__init__)
        parameters = {}

        for param_name, param in signature.parameters.items():
            if param_name == 'self':
                continue

            # Try to resolve parameter type
            if param.annotation != inspect.Parameter.empty:
                try:
                    parameters[param_name] = self.resolve(param.annotation)
                except ValueError:
                    if param.default != inspect.Parameter.empty:
                        parameters[param_name] = param.default
                    else:
                        raise ValueError(f"Cannot resolve parameter {param_name} of type {param.annotation}")

        return cls(**parameters)

# Global container instance
container = DependencyContainer()

def inject(*interfaces: Type) -> callable:
    """Decorator for injecting dependencies.
    
    Args:
        *interfaces: Interfaces to inject
        
    Returns:
        Decorator function
    """
    def decorator(cls: Type) -> Type:
        original_init = cls.__init__

        def new_init(self: Any, *args: Any, **kwargs: Any) -> None:
            # Inject dependencies
            for interface in interfaces:
                setattr(self, f"_{interface.__name__.lower()}", container.resolve(interface))
            
            # Call original init
            original_init(self, *args, **kwargs)

        cls.__init__ = new_init
        return cls

    return decorator