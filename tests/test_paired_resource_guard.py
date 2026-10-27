import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))

from paired_resource_guard import ServiceMetrics, parse_metrics, violations  # noqa: E402


class PairedResourceGuardTests(unittest.TestCase):
    def service(self, *, current_gib: float = 1.0, limit_gib: float = 4.0, full_avg10: float = 0.0, full_avg60: float = 0.0):
        return ServiceMetrics(
            memory_current_bytes=int(current_gib * 1024 * 1024 * 1024),
            memory_limit_bytes=int(limit_gib * 1024 * 1024 * 1024),
            memory_full_avg10_pct=full_avg10,
            memory_full_avg60_pct=full_avg60,
            swap_current_bytes=0,
            oom_kill_events=0,
        )

    def metrics(
        self,
        full_avg10: float,
        full_avg60: float,
        swap_free_gib: float,
        *,
        swap_total_gib: float = 50.0,
        mem_available_gib: float = 10.0,
        memory_full_avg10: float = 0.0,
        memory_full_avg60: float = 0.0,
    ):
        psi = (
            "some avg10=1.00 avg60=1.00 avg300=1.00 total=10\n"
            f"full avg10={full_avg10:.2f} avg60={full_avg60:.2f} "
            "avg300=1.00 total=10\n"
        )
        mem = (
            f"MemAvailable: {int(mem_available_gib * 1024 * 1024)} kB\n"
            f"SwapTotal: {int(swap_total_gib * 1024 * 1024)} kB\n"
            f"SwapFree: {int(swap_free_gib * 1024 * 1024)} kB\n"
        )
        memory_psi = (
            "some avg10=0.00 avg60=0.00 avg300=0.00 total=10\n"
            f"full avg10={memory_full_avg10:.2f} avg60={memory_full_avg60:.2f} "
            "avg300=0.00 total=10\n"
        )
        return parse_metrics(psi, mem, memory_psi)

    def test_quiet_host_has_no_guard_reasons(self):
        self.assertEqual(violations(self.metrics(0.4, 0.8, 8.0), self.service()), [])

    def test_io_psi_and_low_swap_are_informational_not_deployment_blockers(self):
        self.assertEqual(violations(self.metrics(99.0, 99.0, 0.1), self.service()), [])

    def test_sustained_io_psi_does_not_block_with_healthy_memory(self):
        self.assertEqual(violations(self.metrics(0.2, 99.0, 8.0), self.service()), [])

    def test_host_memory_psi_does_not_block_when_grepmesh_cgroup_is_healthy(self):
        host = self.metrics(0.2, 99.0, 0.0, memory_full_avg10=40.0, memory_full_avg60=20.0)
        self.assertEqual(violations(host, self.service()), [])

    def test_malformed_psi_fails_closed(self):
        with self.assertRaises(ValueError):
            parse_metrics(
                "full avg10=0.0",
                "MemAvailable: 10000000 kB\nSwapTotal: 100 kB\nSwapFree: 90 kB",
                "full avg10=0.0 avg60=0.0",
            )

    def test_swapless_host_passes_with_memory_reserve_and_no_memory_pressure(self):
        self.assertEqual(
            violations(self.metrics(0.4, 0.8, 0.0, swap_total_gib=0.0, mem_available_gib=16.0), self.service()),
            [],
        )

    def test_swapless_host_fails_without_memory_reserve(self):
        reasons = violations(
            self.metrics(0.4, 0.8, 0.0, swap_total_gib=0.0, mem_available_gib=3.0), self.service()
        )
        self.assertTrue(any("MemAvailable" in reason for reason in reasons))

    def test_swapless_host_fails_under_memory_pressure(self):
        reasons = violations(
            self.metrics(
                0.4,
                0.8,
                0.0,
                swap_total_gib=0.0,
                mem_available_gib=16.0,
            ),
            self.service(full_avg60=2.0),
        )
        self.assertTrue(any("GrepMesh cgroup memory PSI full avg60" in reason for reason in reasons))

    def test_low_grepmesh_cgroup_headroom_blocks_even_when_host_is_healthy(self):
        reasons = violations(
            self.metrics(0.0, 0.0, 8.0, mem_available_gib=40.0),
            self.service(current_gib=3.75, limit_gib=4.0),
        )
        self.assertTrue(any("GrepMesh cgroup memory headroom" in reason for reason in reasons))


if __name__ == "__main__":
    unittest.main()
