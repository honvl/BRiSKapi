//! Frame-batched auction state for the account-free, historical BRiSK mock.
use anyhow::{Result, bail, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::collections::BTreeMap;
pub mod latency;
use latency::Latency;

#[derive(Debug, Deserialize)]
pub struct Batch {
    pub r#type: String,
    pub seq: u64,
    pub source_time_us: u64,
    #[serde(default)]
    pub source: Option<String>,
    #[serde(default)]
    pub input_transport: Option<Value>,
    #[serde(default)]
    pub trading_date: Option<String>,
    #[serde(default)]
    pub market_issue_count: usize,
    #[serde(default)]
    pub master: Vec<Value>,
    #[serde(default)]
    pub quotes: Vec<Quote>,
    #[serde(default)]
    pub decode_ns: u64,
    #[serde(default)]
    pub received_unix_ms: Option<u64>,
    #[serde(default)]
    pub replay_lateness_ms: Option<f64>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quote {
    pub issue_id: u32,
    pub code: String,
    pub frame: u32,
    pub source_time_us: u64,
    #[serde(flatten)]
    pub fields: BTreeMap<String, Value>,
}

#[derive(Debug, Default, Serialize)]
pub struct State {
    pub source: &'static str,
    pub input_transport: Option<Value>,
    /// True only while an initialized historical replay is running. Never live.
    pub replay_running: bool,
    pub trading_date: String,
    pub market_issue_count: usize,
    pub source_time_us: u64,
    pub next_seq: u64,
    pub batches: u64,
    pub quote_updates: u64,
    pub max_decode_ns: u64,
    pub failure: Option<String>,
    pub master: Vec<Value>,
    pub quotes: BTreeMap<u32, Quote>,
    #[serde(skip)]
    pub latency: Latency,
}

impl State {
    pub fn apply(&mut self, batch: Batch) -> Result<()> {
        let result = self.validate_and_apply(batch);
        if let Err(error) = &result {
            self.invalidate(error.to_string());
        }
        result
    }

    fn validate_and_apply(&mut self, batch: Batch) -> Result<()> {
        ensure!(
            batch.seq == self.next_seq,
            "Stream sequence gap: expected {}, got {}",
            self.next_seq,
            batch.seq
        );
        ensure!(
            self.failure.is_none(),
            "Stream was invalidated; restart from snapshot"
        );
        ensure!(
            batch.source_time_us >= self.source_time_us,
            "Market time regressed"
        );
        match batch.r#type.as_str() {
            "bootstrap" => {
                ensure!(self.next_seq == 0, "Duplicate bootstrap");
                ensure!(
                    batch.source.as_deref() == Some("historical_mock"),
                    "Only historical mock input is supported"
                );
                ensure!(
                    !batch.quotes.is_empty() && batch.master.len() == batch.quotes.len(),
                    "Incomplete bootstrap"
                );
                ensure!(
                    batch.trading_date.as_deref() == Some("20210927"),
                    "Unexpected mock date"
                );
                ensure!(
                    batch.market_issue_count >= batch.master.len(),
                    "Invalid market issue count"
                );
                let mut ids = BTreeMap::new();
                for m in &batch.master {
                    let id = m["issue_id"]
                        .as_u64()
                        .ok_or_else(|| anyhow::anyhow!("Invalid master ID"))?;
                    let code = m["code"]
                        .as_str()
                        .ok_or_else(|| anyhow::anyhow!("Invalid master code"))?;
                    ensure!(ids.insert(id, code).is_none(), "Duplicate master ID");
                }
                for q in &batch.quotes {
                    ensure!(
                        ids.get(&(q.issue_id as u64)) == Some(&q.code.as_str()),
                        "Quote/master identity mismatch"
                    );
                }
                self.source = "historical_mock";
                self.input_transport = batch.input_transport;
                self.trading_date = batch.trading_date.clone().unwrap();
                self.market_issue_count = batch.market_issue_count;
                self.master = batch.master;
                self.replay_running = true;
            }
            "quotes" | "end" => ensure!(self.replay_running, "No initialized running replay"),
            other => bail!("Unknown batch type: {other}"),
        }
        // Validate the whole batch before mutating the latest state. A per-issue
        // frame may advance by several deltas in one network frame; require
        // monotonicity, not a false +1 constraint.
        let mut seen = BTreeMap::new();
        for q in &batch.quotes {
            ensure!(
                seen.insert(q.issue_id, ()).is_none(),
                "Duplicate quote ID in batch"
            );
            ensure!(
                q.source_time_us <= batch.source_time_us,
                "Quote time exceeds market time"
            );
            if batch.r#type != "bootstrap" {
                let old = self
                    .quotes
                    .get(&q.issue_id)
                    .ok_or_else(|| anyhow::anyhow!("Unknown issue ID"))?;
                ensure!(old.code == q.code, "Issue identity changed");
                ensure!(
                    q.frame >= old.frame && q.source_time_us >= old.source_time_us,
                    "Quote frame/time regressed"
                );
            }
        }
        self.quote_updates += batch.quotes.len() as u64;
        self.quotes
            .extend(batch.quotes.into_iter().map(|q| (q.issue_id, q)));
        self.next_seq += 1;
        self.batches += 1;
        self.source_time_us = batch.source_time_us;
        self.max_decode_ns = self.max_decode_ns.max(batch.decode_ns);
        if batch.r#type == "end" {
            self.replay_running = false;
        }
        Ok(())
    }

    pub fn invalidate(&mut self, reason: String) {
        self.replay_running = false;
        self.failure = Some(reason);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn batch(seq: u64, kind: &str, frame: u32) -> Batch {
        serde_json::from_value(json!({"type":kind,"seq":seq,"source":"historical_mock",
            "source_time_us":100 + seq,"trading_date":"20210927","market_issue_count":2,
            "master":[{"issue_id":0,"code":"9434"},{"issue_id":1,"code":"94345"}],
            "quotes":[{"issue_id":0,"code":"9434","frame":frame,"source_time_us":100,
                "indicative_price10":10150,"market_buy_quantity":500},
                {"issue_id":1,"code":"94345","frame":frame,"source_time_us":100}]}))
        .unwrap()
    }

    #[test]
    fn preserves_identity_auction_fields_and_large_frame_steps() {
        let mut s = State::default();
        s.apply(batch(0, "bootstrap", 9)).unwrap();
        s.apply(batch(1, "quotes", 20)).unwrap();
        assert_eq!(s.quotes.len(), 2);
        assert_eq!(s.quotes[&1].code, "94345");
        assert_eq!(s.quotes[&0].fields["market_buy_quantity"], 500);
        s.apply(batch(2, "end", 20)).unwrap();
        assert!(!s.replay_running);
        assert_eq!(s.next_seq, 3);
    }

    #[test]
    fn gaps_invalidate_without_applying_partial_quotes() {
        let mut s = State::default();
        s.apply(batch(0, "bootstrap", 9)).unwrap();
        assert!(s.apply(batch(2, "quotes", 10)).is_err());
        assert!(!s.replay_running);
        assert_eq!(s.quotes[&0].frame, 9);
        assert!(s.apply(batch(1, "quotes", 10)).is_err());
    }

    #[test]
    fn rejects_corrupt_bootstraps() {
        for change in [
            "source",
            "date",
            "master",
            "code",
            "count",
            "duplicate",
            "future",
            "seq",
        ] {
            let mut b = batch(0, "bootstrap", 9);
            match change {
                "source" => b.source = Some("live".into()),
                "date" => b.trading_date = Some("20261007".into()),
                "master" => b.master.clear(),
                "code" => b.quotes[0].code = "wrong".into(),
                "count" => b.market_issue_count = 1,
                "duplicate" => b.master[1] = b.master[0].clone(),
                "future" => b.quotes[0].source_time_us = 999,
                "seq" => b.seq = 1,
                _ => unreachable!(),
            }
            assert!(State::default().apply(b).is_err(), "{change}");
        }
    }

    #[test]
    fn rejects_regressions_unknown_and_duplicate_issues() {
        for change in [
            "frame",
            "time",
            "market",
            "identity",
            "unknown",
            "duplicate",
            "kind",
            "bootstrap",
        ] {
            let mut s = State::default();
            s.apply(batch(0, "bootstrap", 9)).unwrap();
            let mut b = batch(1, "quotes", 10);
            match change {
                "frame" => b.quotes[0].frame = 8,
                "time" => b.quotes[0].source_time_us = 99,
                "market" => b.source_time_us = 99,
                "identity" => b.quotes[0].code = "bad".into(),
                "unknown" => b.quotes[0].issue_id = 7,
                "duplicate" => b.quotes[1] = b.quotes[0].clone(),
                "kind" => b.r#type = "bogus".into(),
                "bootstrap" => b.r#type = "bootstrap".into(),
                _ => unreachable!(),
            }
            assert!(s.apply(b).is_err(), "{change}");
            assert!(!s.replay_running);
            assert_eq!(s.quotes[&0].frame, 9);
        }
        assert!(State::default().apply(batch(0, "quotes", 1)).is_err());
    }
}
