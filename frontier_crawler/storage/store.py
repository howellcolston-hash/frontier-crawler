"""Where engine files live. Two stores with one interface: a local directory, and a Hugging Face dataset repo.

Input contract:
    store.put_many([(local_path, remote_path), ...], message)   upload or copy, all or nothing per call
    store.get(remote_path, local_path) -> bool                  False when the remote file does not exist
    store.list(prefix) -> list of remote paths under that folder
Remote paths are POSIX, relative, and never start with a slash.
A Hugging Face call is one commit per put_many, so callers batch files (a repo slows after a few thousand commits).
"""

from __future__ import annotations

import os
import shutil
from pathlib import Path
from typing import Any, List, Optional, Protocol, Sequence, Tuple



def _check(remote: str) -> str:
    if not remote or remote.startswith("/") or ".." in Path(remote).parts:
        raise ValueError(f"bad remote path: {remote!r}")
    return remote


class Store(Protocol):
    def put_many(self, files: Sequence[Tuple[Path, str]], message: str) -> None: ...

    def get(self, remote: str, local: Path) -> bool: ...

    def list(self, prefix: str) -> List[str]: ...

    def describe(self) -> str: ...


class LocalStore:
    """A directory. Used for tests and for runs with no Hugging Face token."""

    def __init__(self, root: Path) -> None:
        self._root = Path(root)
        self._root.mkdir(parents=True, exist_ok=True)

    def put_many(self, files: Sequence[Tuple[Path, str]], message: str = "") -> None:
        for local, remote in files:
            target = self._root / _check(remote)
            target.parent.mkdir(parents=True, exist_ok=True)
            tmp = target.with_name(f".{target.name}.tmp-{os.getpid()}")
            shutil.copyfile(local, tmp)
            os.replace(tmp, target)

    def get(self, remote: str, local: Path) -> bool:
        source = self._root / _check(remote)
        if not source.is_file():
            return False
        Path(local).parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, local)
        return True

    def list(self, prefix: str) -> List[str]:
        base = self._root / _check(prefix)
        if not base.is_dir():
            return []
        return sorted(
            path.relative_to(self._root).as_posix()
            for path in base.rglob("*")
            if path.is_file() and not path.name.startswith(".")
        )

    def describe(self) -> str:
        return f"local:{self._root}"


class HubStore:
    """A Hugging Face dataset repo. The token comes from the caller, never from a file or a URL."""

    def __init__(self, repo_id: str, token: str, api: Optional[Any] = None) -> None:
        if not repo_id or not token:
            raise ValueError("repo_id and token are required")
        self._repo_id = repo_id
        if api is None:
            from huggingface_hub import HfApi

            api = HfApi(token=token)
        self._api = api

    def put_many(self, files: Sequence[Tuple[Path, str]], message: str) -> None:
        from huggingface_hub import CommitOperationAdd

        if not files:
            return
        operations = [
            CommitOperationAdd(path_in_repo=_check(remote), path_or_fileobj=str(local)) for local, remote in files
        ]
        self._api.create_commit(
            repo_id=self._repo_id,
            repo_type="dataset",
            operations=operations,
            commit_message=message or "engine: add files",
        )

    def get(self, remote: str, local: Path) -> bool:
        from huggingface_hub.errors import EntryNotFoundError

        local = Path(local)
        staging = local.parent / f".hub-{os.getpid()}"
        try:
            fetched = self._api.hf_hub_download(
                repo_id=self._repo_id,
                repo_type="dataset",
                filename=_check(remote),
                local_dir=str(staging),
            )
        except EntryNotFoundError:
            return False
        local.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(fetched, local)
        shutil.rmtree(staging, ignore_errors=True)
        return True

    def list(self, prefix: str) -> List[str]:
        from huggingface_hub.errors import EntryNotFoundError

        try:
            entries = list(
                self._api.list_repo_tree(
                    repo_id=self._repo_id,
                    repo_type="dataset",
                    path_in_repo=_check(prefix),
                    recursive=True,
                )
            )
        except EntryNotFoundError:
            return []
        # Folders have no size attribute. Only files are returned.
        return sorted(entry.path for entry in entries if getattr(entry, "size", None) is not None)

    def describe(self) -> str:
        return f"huggingface:{self._repo_id}"
