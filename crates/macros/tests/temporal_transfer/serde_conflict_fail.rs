use temporalio_macros::TemporalTransfer;

#[derive(serde::Serialize, serde::Deserialize, TemporalTransfer)]
#[temporal(transfer_type = u64)]
struct Model(u64);

impl From<&Model> for u64 {
    fn from(value: &Model) -> Self {
        value.0
    }
}

impl From<u64> for Model {
    fn from(value: u64) -> Self {
        Self(value)
    }
}

fn main() {}
