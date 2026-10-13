//! Bounded, process-local observations. Unknown is never a hardware estimate.
use serde_json::{json, Value};
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

const CLASSES: usize = 16;
const SAMPLES: usize = 16;
const TTL: Duration = Duration::from_secs(1800);

struct Sample {
    at: Instant,
    seconds: f64,
    warm: bool,
    unix: u64,
}
struct Class {
    backend: String,
    key: String,
    samples: VecDeque<Sample>,
}
#[derive(Default)]
pub(crate) struct Ratings {
    classes: VecDeque<Class>,
}

impl Ratings {
    pub fn record(
        &mut self,
        backend: &str,
        key: &str,
        seconds: f64,
        warm: bool,
        unix: u64,
        now: Instant,
    ) {
        if !matches!(backend, "cpu" | "cuda" | "coreml")
            || key.len() > 80
            || !key.starts_with("ocr:")
            || !seconds.is_finite()
            || seconds <= 0.0
        {
            return;
        }
        self.prune(now);
        let mut class = self
            .classes
            .iter()
            .position(|c| c.backend == backend && c.key == key)
            .and_then(|i| self.classes.remove(i))
            .unwrap_or_else(|| Class {
                backend: backend.into(),
                key: key.into(),
                samples: VecDeque::new(),
            });
        if class.samples.len() == SAMPLES {
            class.samples.pop_front();
        }
        class.samples.push_back(Sample {
            at: now,
            seconds,
            warm,
            unix,
        });
        if self.classes.len() == CLASSES {
            self.classes.pop_front();
        }
        self.classes.push_back(class);
    }

    fn prune(&mut self, now: Instant) {
        for c in &mut self.classes {
            while c
                .samples
                .front()
                .is_some_and(|s| now.saturating_duration_since(s.at) >= TTL)
            {
                c.samples.pop_front();
            }
        }
        self.classes.retain(|c| !c.samples.is_empty());
    }

    pub fn snapshot(&mut self, backend: &str, now: Instant) -> Value {
        self.prune(now);
        let mut out = serde_json::Map::new();
        for c in self.classes.iter().filter(|c| c.backend == backend) {
            let has_warm = c.samples.iter().any(|s| s.warm);
            let mut seconds: Vec<_> = c
                .samples
                .iter()
                .filter(|s| !has_warm || s.warm)
                .map(|s| s.seconds)
                .collect();
            seconds.sort_by(f64::total_cmp);
            let median = seconds[seconds.len() / 2];
            let last = c
                .samples
                .iter()
                .rev()
                .find(|s| !has_warm || s.warm)
                .unwrap();
            out.insert(c.key.clone(), json!({"performance_score": 1.0 / median,
                "median_seconds": median, "observations": seconds.len(),
                "basis": "native_jobs_per_second_same_task_class", "warm": has_warm,
                "observed_at": last.unix, "sample_age_seconds": now.saturating_duration_since(last.at).as_secs(),
                "approximate": true}));
        }
        Value::Object(out)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn cold_starts_unknown_and_cpu_gpu_and_task_classes_never_mix() {
        let mut r = Ratings::default();
        let t = Instant::now();
        assert_eq!(r.snapshot("cpu", t), json!({}));
        r.record("cpu", "ocr:image:png:pixels:4mp", 2.0, false, 100, t);
        r.record("cuda", "ocr:image:png:pixels:4mp", 0.2, true, 100, t);
        r.record("cpu", "ocr:pdf:bytes:1m", 20.0, true, 100, t);
        assert_eq!(
            r.snapshot("cpu", t)["ocr:image:png:pixels:4mp"]["performance_score"],
            0.5
        );
        assert_eq!(
            r.snapshot("cuda", t)["ocr:image:png:pixels:4mp"]["performance_score"],
            5.0
        );
        assert_eq!(
            r.snapshot("cpu", t)["ocr:pdf:bytes:1m"]["performance_score"],
            0.05
        );
        r.record("cpu", "ocr:image:png:pixels:4mp", 0.25, true, 101, t);
        assert_eq!(
            r.snapshot("cpu", t)["ocr:image:png:pixels:4mp"]["performance_score"],
            4.0
        );
    }
    #[test]
    fn finite_class_sample_ttl_and_invalid_observation_bounds() {
        let mut r = Ratings::default();
        let t = Instant::now();
        r.record("cpu", "ocr:x", f64::NAN, true, 100, t);
        assert!(r.classes.is_empty());
        for i in 0..40 {
            r.record("cpu", &format!("ocr:{i}"), 1.0, true, 100, t);
        }
        assert_eq!(r.classes.len(), CLASSES);
        for _ in 0..40 {
            r.record("cpu", "ocr:39", 1.0, true, 100, t);
        }
        assert_eq!(r.classes.back().unwrap().samples.len(), SAMPLES);
        assert_eq!(r.snapshot("cpu", t + TTL), json!({}));
    }
    #[test]
    fn excluded_new_cold_observation_cannot_refresh_an_old_warm_rating() {
        let mut r = Ratings::default();
        let t = Instant::now();
        r.record("cpu", "ocr:pdf:bytes:1m", 2.0, true, 100, t);
        r.record(
            "cpu",
            "ocr:pdf:bytes:1m",
            20.0,
            false,
            120,
            t + Duration::from_secs(20),
        );
        let value = &r.snapshot("cpu", t + Duration::from_secs(20))["ocr:pdf:bytes:1m"];
        assert_eq!(value["performance_score"], 0.5);
        assert_eq!(value["observed_at"], 100);
        assert_eq!(value["sample_age_seconds"], 20);
    }
}
