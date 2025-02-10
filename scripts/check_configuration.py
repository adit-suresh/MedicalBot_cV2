import os
import sys
from dotenv import load_dotenv

# Add project root to Python path
project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.append(project_root)

def check_configuration():
    """Check configuration and environment variables."""
    load_dotenv()
    
    print("\nChecking configuration...")
    
    # Required configurations
    configs = {
        'Slack Bot Token': 'SLACK_BOT_TOKEN',
        'Slack Channel': 'SLACK_DEFAULT_CHANNEL',
        'AWS Access Key': 'AWS_ACCESS_KEY_ID',
        'AWS Secret Key': 'AWS_SECRET_ACCESS_KEY',
        'AWS Region': 'AWS_REGION',
        'DeepSeek API Key': 'DEEPSEEK_API_KEY'
    }
    
    missing = []
    available = []
    
    for name, env_var in configs.items():
        value = os.getenv(env_var)
        if not value:
            missing.append(name)
        else:
            available.append(name)
    
    # Print results
    print("\nAvailable configurations:")
    for config in available:
        print(f"✓ {config}")
    
    if missing:
        print("\nMissing configurations:")
        for config in missing:
            print(f"✗ {config}")
        
        print("\nPlease add the missing configurations to your .env file:")
        print("\n.env example:")
        for name, env_var in configs.items():
            if name in missing:
                print(f"{env_var}=your_{env_var.lower()}_here")
    else:
        print("\nAll configurations are available!")

if __name__ == "__main__":
    check_configuration()