from gateway.app import create_app
from gateway.config_loader import default_demo_config

app = create_app(default_demo_config())
