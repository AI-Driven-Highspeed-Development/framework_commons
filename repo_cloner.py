from __future__ import annotations

import os
import shutil
import subprocess
from pathlib import Path
from typing import Callable, Dict, Mapping, Optional, Sequence
from urllib.parse import urlparse


CommandRunner = Callable[[Sequence[str], Path | None, Optional[Mapping[str, str]]], subprocess.CompletedProcess]


class RepoCloner:
	"""Clone git repositories and optionally strip their history."""

	def __init__(self, runner: CommandRunner | None = None) -> None:
		self._runner = runner or self._default_runner
		self._last_error: Optional[str] = None

	@property
	def last_error(self) -> Optional[str]:
		return self._last_error

	def clone(
		self,
		destination: Path | str,
		repo_url: str,
		strip_history: bool = True,
		extra_args: Sequence[str] | None = None,
		env: Optional[Mapping[str, str]] = None,
	) -> bool:
		destination_path = Path(destination)
		print(f"Cloning repository from {repo_url}...")
		self._last_error = None
		try:
			command = ["git", "clone"]
			if extra_args:
				command.extend(extra_args)
			command.extend([repo_url, str(destination_path)])
			self._runner(command, None, env)
		except subprocess.CalledProcessError as error:
			stderr = error.stderr.decode(errors="ignore") if error.stderr else str(error)
			self._last_error = stderr.strip() or str(error)
			print(f"✗ Failed to clone repository: {self._last_error}")
			return False

		if strip_history:
			self._remove_git_history(destination_path)

		print(f"✓ Repository cloned to {destination_path}")
		return True

	@staticmethod
	def _remove_git_history(destination: Path) -> None:
		git_dir = destination / ".git"
		if git_dir.exists():
			shutil.rmtree(git_dir)

	@staticmethod
	def normalize_repo_url(repo_url: str) -> str:
		clean_url = repo_url.lower()
		if clean_url.endswith(".git"):
			clean_url = clean_url[:-4]
		return clean_url

	@staticmethod
	def to_ssh_url(repo_url: str) -> str:
		if repo_url.startswith(("git@", "ssh://")):
			return repo_url

		parsed = urlparse(repo_url)
		if parsed.scheme not in ("http", "https") or not parsed.netloc:
			return repo_url

		repo_path = parsed.path.strip("/")
		if not repo_path:
			return repo_url

		if repo_path.endswith(".git"):
			repo_path = repo_path[:-4]
		host = parsed.netloc.lower()
		if host.endswith("github.com"):
			return f"git@{host}:{repo_path}.git"

		return repo_url

	@staticmethod
	def build_git_env(use_ssh: bool, ssh_key: Optional[str]) -> Optional[Dict[str, str]]:
		if ssh_key:
			env = os.environ.copy()
			env["GIT_SSH_COMMAND"] = (
				f"ssh -i {ssh_key} -o IdentitiesOnly=yes -o StrictHostKeyChecking=accept-new"
			)
			return env

		if use_ssh:
			return os.environ.copy()

		return None

	@staticmethod
	def _default_runner(
		command: Sequence[str],
		cwd: Path | None,
		env: Optional[Mapping[str, str]],
	) -> subprocess.CompletedProcess:
		return subprocess.run(
			command,
			check=True,
			cwd=cwd,
			env=dict(env) if env is not None else None,
			stdout=subprocess.PIPE,
			stderr=subprocess.PIPE,
		)
