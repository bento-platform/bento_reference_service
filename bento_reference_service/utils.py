from .config import config

__all__ = [
    "make_uri",
]


def make_uri(path: str) -> str:
    return f"{config.service_url_base_path.rstrip('/')}{path}"
