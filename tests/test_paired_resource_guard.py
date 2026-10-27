import sys
import unittest
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "deploy"))

from paired_resource_guard import parse_metrics, violations  # noqa: E402


class PairedResourceGuardTests(unittest.TestCase):
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
        self.assertEqual(violations(self.metrics(0.4, 0.8, 8.0)), [])

    def test_current_high_full_psi_and_near_full_swap_are_rejected(self):
        reasons = violations(self.metrics(19.14, 16.83, 3.7))
        self.assertTrue(any("avg10" in reason for reason in reasons))
        self.assertTrue(any("avg60" in reason for reason in reasons))
        self.assertTrue(any("SwapFree" in reason for reason in reasons))

    def test_sustained_psi_is_rejected_even_if_last_ten_seconds_are_quiet(self):
        reasons = violations(self.metrics(0.2, 5.1, 8.0))
        self.assertTrue(any("avg60" in reason for reason in reasons))

    def test_malformed_psi_fails_closed(self):
        with self.assertRaises(ValueError):
            parse_metrics(
                "full avg10=0.0",
                "MemAvailable: 10000000 kB\nSwapTotal: 100 kB\nSwapFree: 90 kB",
                "full avg10=0.0 avg60=0.0",
            )

    def test_swapless_host_passes_with_memory_reserve_and_no_memory_pressure(self):
        self.assertEqual(
            violations(self.metrics(0.4, 0.8, 0.0, swap_total_gib=0.0, mem_available_gib=16.0)),
            [],
        )

    def test_swapless_host_fails_without_memory_reserve(self):
        reasons = violations(
            self.metrics(0.4, 0.8, 0.0, swap_total_gib=0.0, mem_available_gib=3.0)
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
                memory_full_avg60=2.0,
            )
        )
        self.assertTrue(any("memory PSI full avg60" in reason for reason in reasons))


if __name__ == "__main__":
    unittest.main()
