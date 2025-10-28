from __future__ import annotations

import shutil
import subprocess
from pathlib import Path
from typing import Callable, Sequence


CommandRunner = Callable[[Sequence[str], Path | None], subprocess.CompletedProcess]


class RepoCloner:
	"""Clone git repositories and optionally strip their history."""

	def __init__(self, runner: CommandRunner | None = None) -> None:
		self._runner = runner or self._default_runner

	def clone(self, destination: Path | str, repo_url: str, strip_history: bool = True) -> bool:
		destination_path = Path(destination)
		print(f"Cloning repository from {repo_url}...")
		try:
			self._runner(["git", "clone", repo_url, str(destination_path)], None)
		except subprocess.CalledProcessError as error:
			print(f"✗ Failed to clone template: {error}")
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
	def _default_runner(command: Sequence[str], cwd: Path | None) -> subprocess.CompletedProcess:
		return subprocess.run(command, check=True, cwd=cwd)
