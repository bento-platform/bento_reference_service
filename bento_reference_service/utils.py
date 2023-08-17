from .config import Config

__all__ = [
    "make_uri",
]


def make_uri(path: str, config: Config) -> str:
    return f"{config.service_url_base_path.rstrip('/')}{path}"
