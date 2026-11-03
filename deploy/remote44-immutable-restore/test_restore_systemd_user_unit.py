#!/usr/bin/env python3
"""Guard against invalid per-user systemd credential switching during restore."""
from pathlib import Path
import unittest

HERE = Path(__file__).resolve().parent
SERVICE = HERE / "grepmesh-remote44-immutable-restore.service"
GUARD = HERE / "restore_builder_guard.py"

class UserManagerRestoreUnitTests(unittest.TestCase):
    def test_no_user_or_group_switch_in_user_unit(self):
        # The service is installed under ~/.config/systemd/user/, not the system manager.
        unit = SERVICE.read_text()
        directives = [line.strip() for line in unit.splitlines() if line.strip() and not line.lstrip().startswith(("#", ";"))]
        self.assertNotIn("User=roomhacker", directives)
        self.assertFalse(any(line.startswith(("User=", "Group=", "SupplementaryGroups=")) for line in directives))
        self.assertIn("Type=oneshot", directives)
        self.assertIn("ExecCondition=/usr/bin/python3 -B ", unit)

    def test_explicit_identity_guard_and_fail_closed_paths_stay(self):
        src = GUARD.read_text()
        self.assertIn("os.getuid()!=1000", src)
        self.assertIn('if source not in ("inactive","failed") or transfer not in ("inactive","failed"):', src)
        self.assertIn('"-o","StrictHostKeyChecking=yes"', src)

if __name__ == "__main__":
    unittest.main()
