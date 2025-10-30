"""
YAML Utility Module for ADHD Framework

Provides YamlFile class for loaded YAML data operations and YamlUtil class
for file I/O and utility functions.
"""

import re
import shutil
import tempfile
import urllib.error
import urllib.request
import yaml
from pathlib import Path
from typing import Any, Dict, List, Optional, Union
from urllib.parse import urlparse

from .repo_cloner import RepoCloner


class YamlFile:
    """Represents a loaded YAML file with convenient data access methods."""
    
    def __init__(self, data: Dict[str, Any] = None, file_path: Union[str, Path] = None):
        self.data = data or {}
        self.file_path = file_path
    
    def exists_key(self, key_path: str) -> bool:
        """Check if a key exists using dot notation."""
        try:
            keys = key_path.split('.')
            value = self.data
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return False
            return True
        except (AttributeError, TypeError):
            return False
    
    def get(self, key_path: str, default: Any = None) -> Any:
        try:
            keys = key_path.split('.')
            value = self.data
            for key in keys:
                if isinstance(value, dict) and key in value:
                    value = value[key]
                else:
                    return default
            return value
        except (AttributeError, TypeError):
            return default
    
    def set(self, key_path: str, value: Any) -> None:
        try:
            keys = key_path.split('.')
            current = self.data
            
            # Navigate to the parent of the target key
            for key in keys[:-1]:
                if key not in current:
                    current[key] = {}
                elif not isinstance(current[key], dict):
                    current[key] = {}
                current = current[key]
            
            # Set the final value
            current[keys[-1]] = value
        except (AttributeError, TypeError, IndexError):
            pass
    
    def has_required_keys(self, required_keys: List[str]) -> bool:
        return all(self.exists_key(key) for key in required_keys)
    
    def has_value(self, key_path: str) -> bool:
        """Check if a value exists at the given key path (not None)."""
        return self.get(key_path) is not None

    def save(self, file_path: Union[str, Path] = None) -> bool:
        target_path = file_path or self.file_path
        if not target_path:
            return False
        
        try:
            file_path = Path(file_path)
            # Create directory if it doesn't exist
            file_path.parent.mkdir(parents=True, exist_ok=True)
            
            with open(file_path, 'w', encoding='utf-8') as file:
                yaml.safe_dump(self.data, file, default_flow_style=False,
                               allow_unicode=True, sort_keys=False)
            return True
        except (yaml.YAMLError, IOError, UnicodeEncodeError):
            return False

    def to_dict(self) -> Dict[str, Any]:
        return self.data.copy()
    
    def get_value(self, key_path: str, default: Any = None) -> Any:
        """Alias for get() method for consistency with YamlUtil."""
        return self.get(key_path, default)
    
    def validate_structure(self, required_keys: List[str]) -> bool:
        """Validate that the YAML data contains all required keys with non-None values."""
        if not isinstance(self.data, dict):
            return False
            
        return all(self.has_value(key_path) for key_path in required_keys)
    
    def merge(self, override_data: Dict[str, Any]) -> 'YamlFile':
        """Merge this YAML data with override data."""
        try:
            result = self.data.copy()
            for key, value in override_data.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._merge_dict_recursive(result[key], value)
                else:
                    result[key] = value
            return YamlFile(result, self.file_path)
        except (AttributeError, TypeError):
            return YamlFile(self.data.copy(), self.file_path)
    
    def _merge_dict_recursive(self, base: Dict[str, Any], override: Dict[str, Any]) -> Dict[str, Any]:
        """Recursively merge two dictionaries."""
        try:
            result = base.copy()
            for key, value in override.items():
                if key in result and isinstance(result[key], dict) and isinstance(value, dict):
                    result[key] = self._merge_dict_recursive(result[key], value)
                else:
                    result[key] = value
            return result
        except (AttributeError, TypeError):
            return base


class YamlUtil:
    """Utility class for YAML file I/O and common operations."""

    _repo_cloner: RepoCloner = RepoCloner()

    @staticmethod
    def read_yaml(file_path: Union[str, Path]) -> Optional['YamlFile']:
        try:
            file_path = Path(file_path)
            if not file_path.exists():
                return None
                
            with open(file_path, 'r', encoding='utf-8') as file:
                data = yaml.safe_load(file) or {}
                return YamlFile(data, file_path)
        except (yaml.YAMLError, IOError, UnicodeDecodeError):
            raise FileNotFoundError(f"Configuration file '{file_path}' not found or invalid")
    
    @staticmethod
    def read_yaml_from_url_direct(repo_url: str, file_path: str, branch: str = "main") -> Optional['YamlFile']:
        """Fetch YAML content directly from a repository via raw HTTP access."""
        if not repo_url:
            return None

        if not file_path:
            return YamlUtil._fetch_yaml_from_http(repo_url)

        raw_url = YamlUtil.construct_github_raw_url(repo_url, file_path, branch)
        if not raw_url:
            raw_url = YamlUtil._build_generic_file_url(repo_url, file_path)

        return YamlUtil._fetch_yaml_from_http(raw_url)

    @classmethod
    def read_yaml_from_url(
        cls,
        repo_url: str,
        file_path: str,
        branch: str = "main",
        allow_clone_fallback: bool = True,
        clone_root: Optional[Path] = None,
    ) -> Optional['YamlFile']:
        if not repo_url or not file_path:
            return None

        if cls._is_ssh_url(repo_url):
            return cls.read_yaml_from_url_via_clone(repo_url, file_path, clone_root)

        yaml_file = cls.read_yaml_from_url_direct(repo_url, file_path, branch)
        if yaml_file or not allow_clone_fallback:
            return yaml_file

        return cls.read_yaml_from_url_via_clone(repo_url, file_path, clone_root)

    @classmethod
    def read_yaml_from_url_via_clone(
        cls,
        repo_url: str,
        file_path: str,
        clone_root: Optional[Path] = None,
    ) -> Optional['YamlFile']:
        if not repo_url or not file_path:
            return None

        repo_folder = cls._sanitize_repo_folder_name(repo_url)

        if clone_root:
            clone_root.mkdir(parents=True, exist_ok=True)
            target_dir = clone_root / repo_folder
            cleanup_root = clone_root
        else:
            cleanup_root = Path(tempfile.mkdtemp(prefix="adhd_yaml_clone_"))
            target_dir = cleanup_root / repo_folder

        shutil.rmtree(target_dir, ignore_errors=True)
        target_dir.parent.mkdir(parents=True, exist_ok=True)

        if not cls._repo_cloner.clone(target_dir, repo_url):
            cls._cleanup_clone_paths(target_dir, cleanup_root)
            return None

        relative_path = Path(file_path.lstrip('/'))
        yaml_path = target_dir / relative_path
        yaml_file: Optional[YamlFile]
        try:
            yaml_file = cls.read_yaml(yaml_path)
        except FileNotFoundError:
            yaml_file = None

        cls._cleanup_clone_paths(target_dir, cleanup_root)
        return yaml_file
    
    @staticmethod
    def is_url(item: str) -> bool:
        try:
            result = urlparse(item)
            return all([result.scheme, result.netloc])
        except (ValueError, AttributeError):
            return False
    
    @staticmethod
    def get_repo_full_name(url: str) -> Optional[str]:
        """Extract owner/repo from GitHub URL."""
        try:
            # Handle both HTTPS and SSH GitHub URLs
            patterns = [
                r'github\.com[:/]([^/]+/[^/]+?)(?:\.git)?/?$',
                r'github\.com/([^/]+/[^/]+?)(?:/.*)?$'
            ]
            
            for pattern in patterns:
                match = re.search(pattern, url)
                if match:
                    repo_full_name = match.group(1)
                    # Remove .git suffix if present
                    if repo_full_name.endswith('.git'):
                        repo_full_name = repo_full_name[:-4]
                    return repo_full_name
            return None
        except (AttributeError, TypeError):
            return None
    
    @staticmethod
    def get_repo_name(url: str) -> Optional[str]:
        """Extract just the repo name (without owner) from GitHub URL."""
        full_name = YamlUtil.get_repo_full_name(url)
        if full_name and '/' in full_name:
            return full_name.split('/')[-1]
        return None
        
    @staticmethod
    def construct_github_raw_url(repo_url: str, path: str, branch: str = "main") -> str:
        """Convert GitHub repo URL to raw.githubusercontent.com URL for file access."""
        repo_full_name = YamlUtil.get_repo_full_name(repo_url)
        if repo_full_name:
            # Remove leading slash from path if present
            clean_path = path.lstrip('/')
            return f"https://raw.githubusercontent.com/{repo_full_name}/{branch}/{clean_path}"
        return ""

    @staticmethod
    def _sanitize_repo_folder_name(repo_url: str) -> str:
        repo_name = YamlUtil.get_repo_name(repo_url)
        if repo_name:
            base = repo_name
        else:
            base = re.sub(r'[^A-Za-z0-9._-]+', '_', repo_url).strip('_') or 'repo'
        return f"{base}_yaml"

    @staticmethod
    def _build_generic_file_url(repo_url: str, file_path: str) -> str:
        trimmed_repo = repo_url.rstrip('/')
        if trimmed_repo.endswith('.git'):
            trimmed_repo = trimmed_repo[:-4]
        clean_path = file_path.lstrip('/')
        return f"{trimmed_repo}/{clean_path}"

    @staticmethod
    def _is_ssh_url(repo_url: str) -> bool:
        return repo_url.startswith(("git@", "ssh://"))

    @staticmethod
    def _fetch_yaml_from_http(url: str) -> Optional['YamlFile']:
        if not url:
            return None
        try:
            with urllib.request.urlopen(url) as response:
                content = response.read().decode('utf-8')
                data = yaml.safe_load(content) or {}
                return YamlFile(data, url)
        except (urllib.error.URLError, yaml.YAMLError, UnicodeDecodeError):
            return None

    @staticmethod
    def _cleanup_clone_paths(target_dir: Path, root_dir: Optional[Path]) -> None:
        shutil.rmtree(target_dir, ignore_errors=True)
        if root_dir:
            try:
                if root_dir.exists() and not any(root_dir.iterdir()):
                    root_dir.rmdir()
            except OSError:
                pass

    @staticmethod
    def load_init_yaml(file_path: str = "init.yaml", defaults_schema: Dict[str, Any] = None) -> Optional['YamlFile']:
        """Load and validate an init.yaml file with default values."""
        yaml_file = YamlUtil.read_yaml(file_path)
        if yaml_file is None:
            return None

        for key, default_value in (defaults_schema or {}).items():
            if not yaml_file.exists_key(key):
                yaml_file.set(key, default_value)
        
        return yaml_file

    @staticmethod
    def save_init_yaml(data: Dict[str, Any], file_path: str = "init.yaml", required_keys: List[str] = None) -> bool:
        """Save data to an init.yaml file with validation."""
        # Create YamlFile object and validate
        yaml_file = YamlFile(data)

        if not yaml_file.has_required_keys(required_keys):
            missing_keys = [key for key in required_keys if not yaml_file.exists_key(key)]
            print(f"Warning: Missing required keys in YAML data: {missing_keys}")
        
        return YamlUtil.save_yaml(data, file_path)
